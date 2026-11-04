import { type ClaudeCodeHarnessOptions as KernelOptions, claudeCodeHarness as kernelHarness, type ClaudeSandbox } from 'harness-world-claude-code';
import type { Harness } from 'harness-world';
import { isWorkspaceTool } from 'harness-world-hands/runtime';
import { type Agent, type AgentOptions, kernelAgent, splitOptions } from './agent.ts';
import { kernelSandboxOf, resolveEnvironment } from './environments.ts';
import { markSandboxHarness } from './sandbox-harness.ts';
import { isWorkspace, sandbox, type SandboxWorkspace } from './workspace.ts';

/** `claudeCodeHarness`'s options: the kernel harness's, with the session's sandbox given as a `sandbox()` workspace. */
export interface ClaudeCodeHarnessOptions extends Omit<KernelOptions, 'sandbox' | 'executable' | 'unsandboxedCommands' | 'sdk'> {
  /**
   * The session's Vercel Sandbox: the VM Claude Code runs in, with the
   * session's Drive at `/workspace` (its cwd). Default `sandbox()`. Claude
   * Code runs nowhere else: anything but a `sandbox()` workspace is a
   * `TypeError` here.
   */
  workspace?: SandboxWorkspace;
}

export type ClaudeCodeOptions = ClaudeCodeHarnessOptions & AgentOptions;

/** Options removed when Claude Code moved into the session's sandbox (docs/workspaces.md §3.4), and what replaces each. */
const REMOVED: Readonly<Record<string, string>> = {
  cwd: 'the CLI runs in the session\'s Vercel Sandbox, in /workspace (the session\'s Drive): pass workspace: sandbox()',
  host: 'the CLI runs in the session\'s Vercel Sandbox, never as a child of the process that drives the session: pass workspace: sandbox()',
  executable: 'the CLI is the pinned build the sandbox\'s base image carries',
  env: 'pass the variables as sandbox({ env })',
  workspaceNote: 'the harness tells the model what /workspace is itself',
};

/**
 * Claude Code (the Claude Agent SDK and its `claude` CLI) as a durable agent.
 * The CLI runs in the session's Vercel Sandbox (`workspace`, default
 * `sandbox()`), in `/workspace`, the session's Drive; `query()`, the app's
 * tools and the transcript stay in the Function, and the transcript, tool
 * calls and turn state live in the session's log, so a crash anywhere, or a
 * VM that is gone, resumes from the log on another one. Tool-less by
 * default: the app's `tools` are the CLI's only tools. `builtins: true`
 * turns Claude Code's own tools on in the VM (Bash, Read, Edit, Write,
 * NotebookEdit, WebFetch: the pinned allowlist): each call is in the log
 * before it runs and never rerun after a crash; its files are the Drive's.
 * `model` is an AI Gateway id (`anthropic/claude-sonnet-4.5`); the
 * credential (`AI_GATEWAY_API_KEY`, else the Vercel OIDC token) is added in
 * the Function and never reaches the VM. Every other option is the
 * harness's (`instructions`, `tools`, ...), passed through as is.
 *
 * ```ts
 * // lib/agent.ts
 * export const agent = claudeCode({ model: 'anthropic/claude-sonnet-4.5' }) // no tools: it answers from the conversation
 *
 * // or a coding agent on the session's Drive:
 * export const coder = claudeCode({ model: 'anthropic/claude-sonnet-4.5', builtins: true })
 * // app/api/chat/route.ts
 * export const { GET, POST } = agent.chatRoute({ authorize }) // your auth, or 'public'
 * ```
 *
 * Install `@anthropic-ai/claude-agent-sdk` (the version this package pins).
 */
export function claudeCode(options: ClaudeCodeOptions): Agent {
  const [agent, harness] = splitOptions(options);
  return kernelAgent({ ...agent, harness: claudeCodeHarness({ ...harness, ...(agent.log ? { log: agent.log } : {}) }) });
}

/**
 * The kernel harness `claudeCode()` makes, for `durableSessions({ harness })`:
 * the same options and defaults (the session's sandbox is `sandbox()` unless
 * `workspace` says otherwise).
 */
export function claudeCodeHarness(options: ClaudeCodeHarnessOptions = {}): Harness {
  const kernel = kernelOptions(options, 'claudeCodeHarness');
  return markSandboxHarness(kernelHarness(kernel.options), kernel.budget);
}

/** Check `options` and bind the workspace: what the kernel harness takes. Throws a `TypeError` for anything Claude Code can't run with. */
function kernelOptions(options: ClaudeCodeHarnessOptions, fn: string): { options: KernelOptions; budget: { sessionUsd: number; vmSeconds: number } } {
  for (const [key, replacement] of Object.entries(REMOVED)) {
    if (key in options && (options as Record<string, unknown>)[key] !== undefined) throw new TypeError(`durable-harness: ${fn}({ ${key} }) was removed: ${replacement}`);
  }
  // The kernel harness's own binding: only the session factory's attach, through `workspace`, ever reaches it.
  if ((options as { sandbox?: unknown }).sandbox !== undefined) throw new TypeError(`durable-harness: ${fn}({ sandbox }) is internal: pass workspace: sandbox()`);
  if ((options as { unsandboxedCommands?: unknown }).unsandboxedCommands !== undefined) {
    throw new TypeError(`durable-harness: ${fn}({ unsandboxedCommands }) isn't taken: Claude Code's commands always run under its own sandbox in the session's VM (the sandbox() base has what it needs)`);
  }
  // A caller's Agent SDK module could start the CLI anywhere (its query() need not use the sandbox's spawn): the pinned one only.
  if ((options as { sdk?: unknown }).sdk !== undefined) throw new TypeError(`durable-harness: ${fn}({ sdk }) isn't taken: the CLI starts only through the pinned @anthropic-ai/claude-agent-sdk, in the session's sandbox`);
  const { workspace = sandbox(), ...rest } = options;
  if (isWorkspace(workspace) && workspace.kind !== 'sandbox') {
    throw new TypeError(`durable-harness: ${fn}() runs Claude Code only in a Vercel Sandbox: pass workspace: sandbox() (a ${workspace.kind}() workspace is for the harnesses that run in the Function: Pi, pi-durable, fx, OpenCode)`);
  }
  const env = resolveEnvironment(workspace);
  if (env.kind !== 'sandbox') throw new TypeError(`durable-harness: ${fn}() takes workspace: sandbox()`);
  for (const tool of rest.tools ?? []) {
    if (isWorkspaceTool(tool)) {
      throw new TypeError(
        `durable-harness: ${fn}({ tools }) got the workspace tool ${JSON.stringify((tool as { name?: string }).name ?? '?')}: in its sandbox Claude Code uses its own tools on /workspace; pass builtins: true instead (tools takes the app's own tools)`
      );
    }
  }
  const ws = env.workspace;
  const sandboxBinding: ClaudeSandbox = {
    ...kernelSandboxOf(env),
    network: ws.network,
    env: ws.env,
    budget: { turnUsd: ws.budget.turnUsd, sessionUsd: ws.budget.sessionUsd },
    commandTimeoutMs: ws.commandTimeoutMs,
  };
  return { options: { ...rest, sandbox: sandboxBinding }, budget: ws.budget };
}

/** The pinned Agent SDK version (`@anthropic-ai/claude-agent-sdk`). */
export { CLAUDE_AGENT_SDK_VERSION } from 'harness-world-claude-code';
/** `claudeCode({ prepare })`'s hook and what it gets. */
export type { DriveToPrepare, PrepareDrive } from 'harness-world-process';

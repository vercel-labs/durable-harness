# harness-world-claude-code

Durable [Claude Code](https://code.claude.com) on any Workflow SDK World:
world-local, world-postgres or world-vercel. A session is a workflow run and
its event log is the source of truth: Claude Code's transcript, the turn
state and a record of every tool call are in the log, so a crash anywhere
resumes the session from the log, in another process, on another VM.

The `claude` CLI runs **only in the session's Vercel Sandbox**: a VM per
session, with the session's Drive at `/workspace` (its files outlive any
VM). The Agent SDK's `query()`, the app's tools and the transcript stay in
the Function that drives the session. It is tool-less by default: the app's
tools are the CLI's only ones; with `builtins: true` Claude Code's own tools
work on `/workspace` (see [Tools](#tools)).

> A private workspace package: apps install
> [`durable-harness`](../durable-harness), which bundles it as
> `durable-harness/claude-code` (`claudeCode()` attaches the session's
> sandbox, `sandbox()`, by default). It is built on the shared core in
> [harness-world-process](../harness-process/README.md) and the sandbox tier in
> [harness-world-sandbox](../harness-sandbox).

It is a kernel `Harness`, like Pi and fx: `durableSessions({ world, harness })`,
then `start`, `send`, `live`, `inspect`, and `handler` for the queue.

```ts
// with durable-harness (what apps use): the session's sandbox is sandbox() unless you pass one
import { claudeCode } from 'durable-harness/claude-code'
export const agent = claudeCode({ model: 'anthropic/claude-sonnet-4.5', builtins: true })
```

The raw harness takes the session's sandbox as `sandbox` (harness-world-sandbox's
`kernelSandbox` over its session factory, with the descriptor's network,
budget, env and command timeout); without one, or with a machine the session
factory didn't make, a turn fails with `Unavailable('sandbox_required')`.

## How it works

Each drive (one queue delivery):

1. returns at once if there is nothing to do (no open turn, nothing pending);
2. attaches the session's VM (the `sandbox.attach` fact: the Drive, a VM
   that is running or a fresh one on the same Drive, the relay armed with the
   drive's fence), with the VM's model port served by the drive's
   `gatewayHandler`;
3. starts `query()` with a spawner of its own (`sandboxSpawn`): the CLI runs
   in the VM as the unprivileged `agent` user, from the base image's pinned
   build (`CLAUDE_VM_EXECUTABLE`), with an environment built only from the
   VM's paths, the model port and the attach's model key. A resume loads the
   transcript from the log, and only `projects/**` is uploaded to the VM;
4. mirrors every line the CLI writes to its transcript into the log, through
   the SDK's `sessionStore`; the mirror's paths are rewritten from the VM's
   config dir to the Function's (a frame is what parses to one, however its
   bytes are laid out), and a frame that doesn't map, or a batch the SDK
   dropped (`mirror_error`), ends the turn with an `Unavailable`
   (`mirror_unmapped`), the session kept;
5. if the last turn never ended, opens with a recovery note built from the
   log;
6. places each submission in the log, then sends it (a steer goes in
   mid-turn, at priority `next`);
7. records the CLI's tool calls, each keyed by its `tool_use` id
   (`tool:<id>`): its own tools through its hooks, the app's `HostTool`s (an
   in-process MCP server, in the Function) as `ctx.effect` with the tool's
   replay policy. A `never` call runs only once the assistant entry that
   asked for it is in the log;
8. writes the normalized transcript and live events (`text` deltas,
   `tool_start`, `tool_end`, `idle`), then closes the CLI's input.

If the CLI exits mid-turn without the drive stopping it (the link to the VM
dropped, so the relay killed it; an OOM kill), the drive is rebuilt from the
log in the same delivery (the kernel's `Restart`): it re-attaches and
resumes the turn with the recovery note, up to 3 times per delivery while
more than a minute is left; past that the delivery fails and the queue
redelivers.

### Tools

**Tool-less by default**: the app's `HostTool`s (`tools`) are the CLI's only
tools (`tools: []`), and the system prompt says so (`TOOLS_NOTE`, or
`NO_TOOLS_NOTE` with no tools at all).

**With `builtins: true`**, Claude Code's own tools are on in the VM, on
`/workspace`: the pinned allowlist `CLAUDE_BUILTINS` (Bash, Read, Edit,
Write, NotebookEdit, WebFetch), checked against the pinned CLI's
`system/init` tools by a test, so a bump that adds a tool fails until it is
classified. `builtins` also takes a list of names, or `{ type: 'preset',
preset: 'claude_code' }`. Whatever it says, `CLAUDE_BUILTINS_OFF` stays off:
the tools whose state the transcript does not carry (cron, scheduled
wakeups, worktrees, agent teams, background shells, workflows, the task
list, which 2.1.296 keeps in its config dir), subagents (`Task`, `Agent`:
their sidechains' resume across VMs isn't proven yet) and the ones that
wait for a user. WebSearch is provider-hosted, so it is off unless `providerTools`
lists `web_search`. Nothing asks for permission (`permissionMode:
'dontAsk'`): a call runs when the `PreToolUse` hook allows it, and the hook
allows it only once the call's intent is in the log.

- **Every call is in the log before it runs**, and its outcome after it;
  the outcome of a call that may have written waits for the Drive's writes
  to be flushed (`FSYNC`). A flush that fails is never recorded as a
  success: the call is committed failed, saying its writes may not be on
  the Drive, and the model is told the same.
- **After a crash, nothing reruns a call.** The resumed CLI answers a call
  that was in flight "interrupted", and the recovery note says the same from
  the log ("may have taken effect: check before repeating it", or "safe to
  repeat" for a read). A call id the log already finished is refused.
- **Files are the Drive's**: durable across VMs, not transactional (a
  command a crash interrupted may have half-run, and the system prompt
  says so, `DRIVE_NOTE`). A lost Drive is said once (`FILES_LOST_NOTE`).
- **A command never outlives its invocation.** Bash's timeout is clamped to
  what is left of it (and to `commandTimeoutMs`); a command the clamp ends
  has a definite outcome ("ran out of time in this invocation"). A
  background command is refused.
- **Claude's own sandbox** runs inside the VM (a bubblewrap self-test at
  each drive): the model's commands get no model key, can't see or trace
  the CLI (a pid namespace), can't reach the model port (by any loopback
  name the proxy knows) or read the CLI's config dir, and write only
  `/workspace` and their temp dirs. **It is required**: where it can't
  start, the turn ends with an `Unavailable`. Without it a command would
  run beside the CLI as the same user, read the model key from its
  environment and take its socket, so the key would be only a correlation
  id; `unsandboxedCommands: true` (the raw harness only, for the stand-in
  VM's tests) accepts that.
- **Its network follows the VM's.** The session factory applies
  `sandbox({ network })` to the VM on every attach (a per-session callback's
  result included); Claude's own sandbox lets commands reach what the
  policy names: everything for `allow-all` or a policy that allows `*` (the
  VM's policy, rules and all, still applies past it), the listed domains
  for `{ allow }` or a full policy, nothing for `deny-all`. It never sees
  the policy's rules, so no header value reaches the CLI.
  A domain with a wildcard inside it (`api.*.example.com`) is passed on
  as written; if Claude's sandbox does not match it the way the VM's
  firewall does, its commands are refused there, never let further.
- **The file tools don't write `/workspace/.git` or `/workspace/.claude`**,
  and the CLI gathers no git status of its own (`CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS`):
  it would run git in `/workspace` outside its sandbox, and git runs what a
  planted `.git/config` names.

**The model path.** The CLI's base URL is the VM's model port, and its key
the attach's model key: the relay admits a request only with that key and,
where it can read `/proc`, only from a process the attach spawned. Each
request goes through the tunnel to the drive's `gatewayHandler`, which
matches it against a fixed table (`CLAUDE_ROUTES`), checks the body and the
model, adds the Gateway credential (`credential()`, default
`AI_GATEWAY_API_KEY`, else the Vercel OIDC token) and counts its cost against
`sandbox({ budget })` in the log (`journalLedger`). No credential is ever in
the VM.

## Install

```bash
pnpm add durable-harness @anthropic-ai/claude-agent-sdk@0.3.296 zod@^4
```

`@anthropic-ai/claude-agent-sdk` is a peer dependency, pinned exactly: the
transcript format the mirror carries is CLI-internal, and the VM's CLI
(2.1.296) is the one this SDK version ships. Another version is refused at
the first drive with an `Unavailable` naming both versions and the install
line. A golden transcript the previous pin wrote is resumed by the current
one in the tests (`test/fixtures/golden-2.1.289.json`).

## Options

| Option | Default | |
| --- | --- | --- |
| `sandbox` | (required) | The session's sandbox: `durable-harness`'s `claudeCode()` builds it from `workspace: sandbox()` |
| `tools` | none | The app's tools, over MCP, in the Function |
| `builtins` | `false` | Claude Code's own tools in the VM (see [Tools](#tools)) |
| `disallowedTools` | | More built-ins to turn off, on top of `CLAUDE_BUILTINS_OFF` |
| `builtinReplay` | `claudeBuiltinReplay` (reads `safe`) | A record or a function: what the recovery note says about an interrupted built-in, and whether its outcome waits for `FSYNC` |
| `providerTools` | none | Provider-hosted tools (`web_search`, `web_fetch`); refused when the session's network is `deny-all` (a callback's result included) |
| `model` | `claude-sonnet-4-5` | A native Anthropic id, or a Gateway id (`anthropic/claude-sonnet-4.5` becomes `claude-sonnet-4-5`) |
| `smallModel` | `model` | The CLI's background calls; allowlisted at the gateway |
| `instructions` | | Appended to Claude Code's system prompt |
| `credential` | `gatewayCredential` | The Gateway credential, read per model request in the Function |
| `gatewayUrl` | `AI_GATEWAY_BASE_URL` or `https://ai-gateway.vercel.sh` | |
| `executable` | `CLAUDE_VM_EXECUTABLE` | The CLI's path in the VM (tests on the stand-in VM pass the host build) |
| `name` | `claude-code` | The kernel name |
| `sdk` | `import('@anthropic-ai/claude-agent-sdk')`, version-checked | The SDK module |
| `barrierMs` | 10 000 | How long a `never` tool waits for its call to reach the log before it is refused |
| `coalesceMs` | 150 | How long mirrored lines are held so they share journal batches |
| `idleMs` | 0 | Keep the child this long after a turn for a follow-up |
| `maxTurns` | | `query()`'s `maxTurns` |
| `unsandboxedCommands` | `false` | Run the built-ins' commands without Claude's sandbox where bubblewrap doesn't work (the key is then only a correlation id); the stand-in VM's tests. `durable-harness` refuses it |
| `log` | | Diagnostics |

Removed when the CLI moved into the VM: `cwd`, `host`, `env` and
`workspaceNote` (`durable-harness` throws a `TypeError` naming the
replacement: `workspace: sandbox()`, `sandbox({ env })`).

## What is in the log

| Record | Written as | Key |
| --- | --- | --- |
| The attach | a `never` effect `sandbox.attach` (the `hw.sandbox` document's edits commit with it) | `sandbox.attach:<uuid>` |
| A placed submission | a record-only effect `place` | `place:<first submission id>` |
| Transcript lines and normalized transcript entries | journal batches, a record-only effect `journal` | `journal:<n>` (streams `claude`, `claude/<subpath>`, `transcript`) |
| Model spend | journal records (stream `hw.spend`), per turn and session | |
| A tool call | `tool.<name>`, its input, its outcome | `tool:<tool_use id>` |
| A note the model was shown (a lost Drive), acknowledged | a `safe` effect `sandbox.notes` | `sandbox.notes:<uuid>` |
| A model request, only around a "Crash during: model" drill | a record-only effect `model` | `model:<uuid>` |

## Tests

`pnpm --filter harness-world-claude-code test`, no credentials:

- `test/harness.test.ts`: the harness's own logic with a scripted SDK on the
  sandbox tier's stand-in VM (the real session factory over local relays):
  the refusals, the mirror, the bridge, the barrier, steering, the hooks
  (the Bash clamp, the background refusal, FSYNC), crashes mid-tool and the
  files-lost note.
- `test/sandbox-restart.test.ts`: the **real Agent SDK and the pinned CLI**
  on the stand-in VM, against a Messages API fake that enforces the API's
  structure (`test/fake-anthropic.ts`): restart on a new VM (R1-R8: a file
  and a code word across VMs, the VM killed mid-Bash, the driver killed and
  the VM taken over, a kill mid model stream, `/ctl` or `/model` dropped,
  the CLI OOM-killed, the Drive held by the old VM session, three VMs in a
  row, a resume past a compaction, the Drive deleted, a zombie driver
  waking after a takeover, a crash between a Write's FSYNC and its outcome),
  the allowlist against the CLI's init tools, the golden transcript from
  2.1.289, the `cli_version` downgrade refusal, an unmapped mirror frame,
  planted settings and a planted `.git/config`, the Bash clamp, a failed
  FSYNC, and the spend caps (a turn's and the session's, across
  invocations). The `/ctl`, `/model` and OOM cases assert no failed
  delivery: they recover in the same one.
- `test/env.test.ts`, `test/versions.test.ts`, `test/store.test.ts`,
  `test/transcript.test.ts`: the environment, the spawn's checks, the pins,
  the store and the transcript.

Live (`DH_LIVE_SANDBOX=1`, run from a directory linked to the project):
`test/live-drills.test.ts` runs Claude Code in a real VM with the real model
through the Gateway (L1, L3, L4, L5, L8, L9 and P0.7; L4 kills a driver
process, `test/live-driver.ts`).

## Not done yet

- **Subagents** (`Task`, `Agent`) are off, even under the preset: their
  transcripts would be mirrored as sidechains, but no test resumes one.
- **Concurrent tool calls**: the app's tools are serialized; Claude Code's
  own may run in parallel.

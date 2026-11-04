# harness-world-codex

[Codex](https://github.com/openai/codex), durable: `codex app-server` driven
as a kernel `Harness` on harness-world, so a Codex session is a workflow run
whose event log is the source of truth. **Codex runs only in the session's
Vercel Sandbox VM**: its process,
its shell and its files are there, on `/workspace` (the session's Drive),
while the drive, the log and the Gateway credential stay in the Function.
A crash anywhere (the Function, the VM) resumes the session from the log,
on the same VM or a new one on the same Drive, with Codex's own thread
restored and every tool call accounted for. It is tool-less by default:
the app's tools are Codex's only ones. With `builtins: true`, Codex's own
shell and `apply_patch` work on the Drive, under Codex's own sandbox in the
VM (see [Tools](#tools)).

> A private workspace package: apps install
> [`durable-harness`](../durable-harness), which bundles it as
> `durable-harness/codex` (`codex({ model })`, whose `workspace` defaults to
> `sandbox()`). Pinned to `@openai/codex` 0.160.0 (its app-server protocol
> is marked experimental and changes between minors); the VM image carries
> the linux-x64 build (harness-world-sandbox's base recipe).

It is built on the shared core for process harnesses,
[harness-world-process](../harness-process/README.md), and the sandbox tier,
[harness-world-sandbox](../harness-sandbox), and has the same user DX as
every other harness here: `durableSessions({ world, harness })`, then
`sessions.start`, `send`, `live` and `handler`.

## What it does

- **Tool-less by default.** The `HostTool`s you pass are app-server
  dynamic tools, each call the kernel's effect sandwich keyed by Codex's
  own `call_id` (`tool:<call_id>`) with the tool's replay policy, and they
  are its only tools (e.g. `harness-world-hands`'s log-backed workspace
  tools). With `builtins: true`, **Codex's own shell and `apply_patch` are
  on** next to them, each call in the log before it runs (see
  [Tools](#tools)), and `view_image` (a read). Image generation, web
  search, sub-agents and plugins stay off either way.
  The `functions` namespace stays directly exposed even for newer code-mode
  model families, preserving each tool's rollout call ID for the journal.
  The nested code-mode host stays disabled.
- **Codex's thread is mirrored into the log.** Codex keeps a thread as an
  append-only JSONL file, the rollout. Every line it writes becomes a record
  of the session's native journal (group-committed record-only effects),
  flushed before every `replay: 'never'` tool and at each turn's end.
- **Every drive restores from the log.** The session's `CODEX_HOME` in the
  VM (`/var/lib/dh/s/codex-home`) is wiped and gets a fresh `config.toml`
  and the mirrored rollout, byte for byte (the relay's `WIPE` and `FILES`);
  app-server resumes the thread by id. A home is never reused (Codex's
  SQLite stores next to it hold absolute paths and byte offsets). The
  rollout comes back over the relay's `TAIL` (inode and size tracked: a
  replaced or shrunk file stops the drive), drained (`SYNC`) before every
  `never` call and at each turn's end.
- **Recovery.** A turn a crash cut off is resumed natively. Calls the rollout
  left without an output get one injected from the log
  (`thread/inject_items`): the committed result if the call finished, else
  "interrupted, may or may not have taken effect". Then a recovery note built
  from the log starts a turn: finished calls with their outputs, interrupted
  `never` calls ("may have taken effect: check before repeating"),
  interrupted `safe` calls ("safe to repeat").
- **The inbox.** Each submission is placed in the log, then sent with its id
  as Codex's `clientUserMessageId`: `turn/start`, or `turn/steer` for a steer
  that arrives while a turn runs. A steer Codex refuses (its turn ended
  first) gets the next Codex turn, inside the same session turn.
- **Credentials.** Model traffic goes from the VM's loopback model port
  (`http://127.0.0.1:4100/v1`, Codex's `base_url`) through the relay's
  `/model` socket to the drive's `gatewayHandler` (harness-world-process,
  `CODEX_ROUTES`: the fixed path table, the body policy, the model, the
  spend caps), which adds the credential (`AI_GATEWAY_API_KEY`, else the
  Vercel OIDC token) in the Function. The VM never holds it. Codex's
  provider key (`env_key`) is the attach's model key (the session factory's
  `modelKey`), kept out of the model's commands' environment by
  `shell_environment_policy`. The key alone is no proof of the caller (a
  command can read app-server's own `/proc/<pid>/environ`): the relay also
  admits a model request only from a process the attach spawned, by its socket
  (A5), and the release gate refuses a relay that can't (`modelAuth`).
  The body policy forces `store: false` and always sets a clamped
  `max_output_tokens`.
- **Spend is in the log.** Each response's cost is a `codex.spend` record
  tagged with its session turn, so `sandbox({ budget: { turnUsd,
  sessionUsd } })` hold across invocations and machines (a crash can lose
  the last group commit's costs). A refused call ends the turn with an
  `Unavailable` naming the cap.
- **app-server's git sees nothing of the Drive's.** app-server runs git
  itself, outside bwrap and with the key in its environment: `GIT_DIR`
  points nowhere and no global or system git config is read, so a
  `.git/config` (or `~/.config/git/config`) the model writes can't make it
  run a command.
- **Self-destruct.** When the drive stops (preempted, out of budget, a
  journal write refused), the session's processes in the VM are killed (its
  cgroup) before anything else is answered. A link to the VM lost mid-drive
  is re-attached in the same invocation, a bounded number of times and only
  while there is work left, once the app's tools still running in the
  Function have settled (so the next drive reads their real outcome), and
  the drive resumes from the log. A rollout replaced or shrunk under the
  tail stops the drive; the third time in a turn ends the turn with an
  `Unavailable`.

## Tools

**With `builtins: true`, Codex's own tools are on**: its shell (unified
exec, `exec_command`; `apply_patch` runs through it) on `/workspace`, under
Codex's sandbox in the VM (`sandboxMode`). Every command and patch asks the
drive first (`approvalPolicy: 'untrusted'`, which asks even for a read),
and the drive answers only once the call's intent is in the log.

- **Every call is in the log before it runs.** The drive drains the
  rollout tail and looks for the call's `function_call` line (a bounded
  number of times, as Codex's rollout writer is asynchronous); only once it
  is in the log does `item/started` commit the intent (`tool.shell` or
  `tool.apply_patch`, keyed `tool:<call_id>`). Without it the approval is
  declined, nothing runs, and the model is told to make the call again. The
  approval is accepted once the intent is committed, once per call. Every
  other approval is declined: a second one for the same call (Codex's
  "retry without sandbox?"), input to a running command (`kind:
  'writeStdin'`), a sub-command (an `approvalId`), network access, more
  permissions (`additionalPermissions`, a `grantRoot`). `item/completed`
  commits the outcome, after the Drive is flushed (`sync -f /workspace`). A
  command whose actions Codex parses as reads, listings or searches only is
  `safe`; everything else `never`.
- **After a crash, nothing reruns a call.** The resumed thread gets the
  call's output injected from the log: the committed result if it finished,
  else "interrupted, may or may not have taken effect". The recovery note
  says the same and asks the model to check before repeating.
- **What a call did is on the Drive, not in the log.** The Drive is
  durable, not transactional: a command a crash interrupted may have half
  run.
- **A command does not outlive its invocation.** A command approved with
  less than 25 s left is declined, never run ("not run: too little time
  was left"). One still running near the invocation's end (or past
  `sandbox({ commandTimeoutMs })`) has its own turn interrupted
  (`turn/interrupt`), and its outcome, when Codex reports it ended, says
  "ran out of time in this invocation". Nothing the model started outlives
  the drive: the relay kills the session's cgroup, and only then are the
  calls still open recorded as interrupted.
- **The sandbox.** The first drive with the built-ins runs `codex sandbox
  -P :workspace -C <an empty dir> -- true` in the VM as the session's user,
  and records the verdict in the log (`codex.sandbox`); every later drive
  keeps it, so nothing on the Drive (or a slow VM) can move a session to
  `danger-full-access`. Pass (P0.10): `workspace-write`, with the network on
  and `/workspace` and `HOME` as its writable roots; `CODEX_HOME` is not
  one. A definite failure: `danger-full-access` (the VM is the boundary and
  holds no credential). No verdict (it didn't finish, twice): the turn
  fails, and the next one tries again.
- **Planted config does nothing.** `/workspace` is marked untrusted in
  `config.toml`, so a `.codex/` the model writes there is never loaded.

## Where it runs

`durable-harness/codex` resolves `workspace: sandbox()` (the default) into
the sandbox tier's session factory, and passes the descriptor's `env`,
`network`, `budget` and `commandTimeoutMs` on as `machine`:

```ts
// lib/agent.ts
import { codex } from 'durable-harness/codex'
export const coder = codex({ model: 'openai/gpt-5.4-mini', builtins: true })
```

- On Vercel the Function drives the session and the VM runs Codex; nothing
  of Codex is traced into the Function. A deployed Function needs a
  prebuilt relay bundle (A7) or `sandbox({ image })`.
- Locally, from a directory `vercel link` linked: the laptop drives, a real
  Vercel Sandbox runs Codex (`examples/local.ts`).
- The raw harness takes the factory's acquire itself
  (`codexHarness({ machine: { acquire } })`); without one, or with a
  machine the factory didn't brand, a drive fails its turn with
  `Unavailable('sandbox_required')`.

**Preparing the Drive.** `prepare` is the app's own setup of the session's
Drive (a repository, its install), run in the drive right after the attach
and before Codex starts, once per Drive: the session's first attach, and
again on a new Drive. Its workspace's commands may carry a credential in
their `env`, before anything else runs on the attach, and the session's
processes are killed when it returns. It is the same hook as Claude
Code's, with the same rules (it must be idempotent; a failure fails the
turn and the next message runs it again): see
[harness-world-claude-code's Preparing the Drive](../harness-claude-code/README.md#preparing-the-drive).
Codex's commands run as the same user with no pid namespace of their own,
which is why a credential goes only into `prepare`'s commands, never into
one that runs while Codex does.

## Using a session (every World)

```ts
const { sessionId } = await sessions.start({ harness: 'codex' })
await sessions.send(sessionId, 'Fix the failing test')
await sessions.send(sessionId, 'Also update the changelog', 'steer') // into the running turn
for await (const event of sessions.live(sessionId)) {
  // { type: 'text', delta } | { type: 'tool_start', name, args, callId } | { type: 'tool_end', ... } | { type: 'idle' }
}
```

The transcript is the normalized `transcript` journal stream
(`transcriptOf(state)` from `harness-world-process/records`): assistant
messages with their tool calls, recovery notes, turn ends. Tool calls are
their `tool:<call_id>` effects.

## In a browser tab

A tab can't run Codex: it runs only in the session's Vercel Sandbox. A tab can
watch a session a server drives, through `harness-world-remote`: a World
whose storage and streams are the server's (`createWorldHandler` on the
server, `createRemoteWorld` in the tab), so the tab reads the same log and
the same live stream. Sends go to the server, which drives.

## Options

| Option | Default | |
| --- | --- | --- |
| `tools` | none | The app's tools, run in the Function: its only tools, or (`builtins: true`) next to the built-ins. Fixed for a thread at its start |
| `prepare` | | The app's setup of a new Drive, before Codex starts (see [Where it runs](#where-it-runs)) |
| `builtins` | `false` | Codex's shell and `apply_patch` on `/workspace`, each call recorded (see [Tools](#tools)) |
| `sandboxMode` | `read-only` with the built-ins off; with them, `workspace-write` if bwrap's self-test passes in the VM, else `danger-full-access` | Codex's sandbox for its commands (was `sandbox`; needs `builtins: true`) |
| `model` | `openai/gpt-5.4-mini` | An AI Gateway model id |
| `instructions` | none | Developer instructions for the thread |
| `baseInstructions` | Codex's own | They describe its built-in tools; replace them if you like |
| `effort` | Codex's | Reasoning effort per turn |
| `credential` | `gatewayCredential` | Read per model request in the Function, in the drive's async context |
| `gatewayUrl` | `AI_GATEWAY_BASE_URL` or `https://ai-gateway.vercel.sh` | |
| `contextWindow` | 272,000 | Gateway model ids are unknown to Codex |
| `config` | none | Extra `config.toml` settings, merged over the generated ones; `RESERVED_CONFIG_KEYS` (the sandbox, approvals, the commands' env, the provider, trust, MCP, notifications, features, web search, the model) throw |
| `name` | `codex` | The kernel name (runs are `workflow//durable-harness@<version>//<name>`) |
| `inboxPollMs` | 500 ms, or 5 s with the World's hints | How often a running turn reads the inbox for steers |
| `log` | none | Diagnostics: stdout noise, declined calls, the sandbox's lifecycle |
| `machine` | none | Internal: the session's VM (`{ acquire, env, network, budget, commandTimeoutMs }`), from `sandbox()` |

`sandbox`, `host`, `cwd`, `bin`, `env`, `keepSessionDir` and `workspaceNote`
are gone and throw, naming what replaced them.

## What a crash costs

- **The model call in flight.** Its partial output is not in the log; Codex
  makes the call again.
- **One recovery turn**: the note, so about one more model request.
- **A tool in flight** gets its replay policy: a `never` tool comes back
  interrupted and is never run again (the model is told it may have taken
  effect); a `safe` one is reported safe to repeat. A built-in command is
  killed with the drive, so it may have partly run.
- **Nothing of the files.** They are on the Drive, which every VM of the
  session mounts; only a deleted Drive loses them (the model is told once).
- **Rollout lines written since the last flush**: the last few completed
  items at most, never a `never` tool's call record (it is flushed before
  the tool's intent).

**Model crash drills.** Codex makes its model calls itself, so they are not
steps of the log, and the kernel's chaos hook only fires inside a step. The
drive's gateway handler sees each Responses request start and end, and while a
crash that targets the model (`at: 'model'` or `'any'`) is armed it writes a
record-only `model` step around it (`ModelSteps` in harness-world-process),
which the crash fires in, `afterMs` after the request started. The recovered
turn's first model request retries the killed step (attempt 2). Without a
drill armed, no model step is written.

## Tests

```bash
pnpm --filter harness-world-codex test
```

Everything runs on the sandbox tier's stand-in (`test/stand-in.ts`): the
session factory over harness-world-sandbox's fake Sandbox API, where each
"VM" is a local relay child and each "Drive" a temp dir, with the fake
app-server (`test/fake-app-server.mjs`, the protocol subset and the rollout
format, its model `fakeModel()` behind the drive's gateway handler) as the
CLI.

- `test/rollout.test.ts`: the mirror's pure parts on rollouts Codex 0.160.0
  wrote (`test/fixtures`), the dangling-call repair, the owed submissions,
  `config.toml`.
- `test/harness.test.ts`: a turn with a tool and a resume on a new VM, a
  crash mid `never` tool and a "Crash during: model" drill each recovered on
  a new VM, an abandoned model step closed without a VM, a crash with the VM
  up (the newer owner's arm kills the stale app-server), steers, a refused
  journal write, a lost link re-attached in the same invocation; with the
  built-ins: a shell command and a patch on the Drive, a crash mid command.
- `test/sandbox-restart.test.ts` (A6): the tunnel and the VM's config; a
  call's line in the log before its intent; a lagging tail declined; a
  shrunk or replaced rollout, and one tampered with in every delivery; an
  escalation declined; a retry without the sandbox declined; input to a
  running command declined; the placeholder hidden from commands and
  `sandbox({ env })` reaching them; a planted `.codex` ignored; the bwrap
  self-test failing, and its verdict kept; a file written, the VM torn
  down, `thread/resume` recalling a code word; a lost Drive told once; a
  spend cap, and one across invocations; a command past its time, and one
  asked for too late; the reserved keys and the removed options; no branded
  machine.
- `test/restart.test.ts` (§11.4): a driver process SIGKILLed between turns
  and mid-command, recovered by another process on a new VM over the same
  Drive (`drive-child.ts`); the VM killed mid-command; a stalled driver
  after a takeover; `/ctl` and `/model` dropped alone; a crash between a
  command and its outcome with the Drive still held; three VMs and a
  compaction; an app tool still running when the link drops.
- `test/live.test.ts`: the real binary on the stand-in, its model on the AI
  Gateway, skipped without a credential (`CODEX_LIVE=1` from a linked
  directory): a tool turn and a resume on a new VM; the built-ins on the
  Drive; a planted repository (`core.fsmonitor`), project config and trust
  entry with no effect; a command that outlives its yield, waited on. Every
  request Codex sends through the model port is logged (P0.7).
- `test/live-sandbox.test.ts` (L2, `DH_LIVE_SANDBOX=1`): on Vercel Sandbox:
  VM A, a hostile model's probes, VM A deleted under a running command (a
  re-attach to VM B, the dangling call answered through
  `thread/inject_items`), then a new process on VM B.

## Not done yet

- **Input to a running command** (`write_stdin` to a unified-exec session):
  an empty poll asks no approval and has no intent of its own (it is part
  of its command's call; after a crash it is answered "interrupted, the
  process went with the drive"); input that asks for approval is declined.
  `view_image` is not recorded either (a read).
- **The model port's key and peer check** (A5): until then the factory
  relays model calls only in tests. The harness already hands the attach's
  key to Codex when the machine carries one.
- **A golden rollout from a previous pin** resuming on the VM's pin: there
  is none before 0.160.0 in the image.
- **Interrupting a turn** from the inbox: the kernel has no interrupt
  submission kind.
- **Keeping app-server up between deliveries**: every drive starts one and
  restores the thread.

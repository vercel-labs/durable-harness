# harness-world-opencode

[OpenCode v2](https://github.com/anomalyco/opencode) (`@opencode/sdk` 2.0.x,
not `opencode-ai` 1.x) as a durable harness on harness-world: a session is a
workflow run on a World (world-vercel, world-postgres, world-local), OpenCode
runs its own agent loop in the drive's process, and a crash anywhere resumes
the turn in another process from the run's log. Tool-less by default: the
app's tools are the only ones. With a workspace, OpenCode's own shell, read,
write, edit, patch, glob and grep run on it: OpenCode's own tool code, its
files and commands on the workspace (a virtual one, or the session's own
Vercel Sandbox), never on the machine that drives the session (see
[Tools](#tools)).

> A private workspace package: apps install
> [`durable-harness`](../durable-harness), which bundles it as
> `durable-harness/opencode`.

It is a kernel `Harness`, built on [`harness-world-process`](../harness-process)
like the other process harnesses (Claude Code, Codex), so it has the
same DX as Pi and fx: `durableSessions({ world, harness })`, then
`sessions.start` / `send` / `live` / `handler`.

```ts
const sessions = durableSessions({ world, harness: opencodeHarness({ tools }) }) // the app's tools: the session's only ones
export const POST = sessions.handler // the flow route
```

Apps write the same with durable-harness's descriptors, which it resolves
for the harness:

```ts
import { virtual } from 'durable-harness'
import { opencode } from 'durable-harness/opencode'
export const agent = opencode({ model: 'anthropic/claude-haiku-4.5', workspace: virtual(), tools }) // OpenCode's own tools on a virtual workspace
// workspace: sandbox() (from durable-harness): the same tools in the session's own Vercel Sandbox
```

Pinned to OpenCode **2.0.22** (`@opencode/client|core|plugin|schema|server|sdk`)
and `effect` 4.0.0-rc.112: the harness uses OpenCode internals (`Bus.replay`,
`createEmbeddedRoutes`, `SessionRestart`, and the services it replaces to keep
OpenCode off the machine) that move between releases; `test/boot-and-tools.test.ts`
gates every bump. Node 24, POSIX. Before it starts OpenCode, each drive (when
the harness runs from an installed package, as durable-harness from npm does)
checks the installed `@opencode/client|core|schema|server` and `effect`
against these pins (pnpm only warns on a peer mismatch): another version fails
the turn as unavailable, with the installed and pinned versions and the install
line, and the session's next message drives it again once the install is fixed.

## Models through AI Gateway

OpenCode reads the selected model's current Gateway metadata through the
drive's authenticated loopback relay and supplies it as a model definition to
the built-in `vercel` provider. It keeps using the same relay and placeholder
credential for inference, with no separate models.dev fetch.

If discovery is unavailable, OpenCode falls back to its bundled model
definitions. A new ID can still be absent from that fallback; catalog presence
alone does not establish a working tool/result round trip.

## How it works

OpenCode v2 is event-sourced: every change to a session is a durable event
with a per-session `seq`, committed in its SQLite before it is published. The
harness makes the run's log the source of truth for those events, and
OpenCode's SQLite a cache that every drive rebuilds.

| Piece | What it does |
| --- | --- |
| The host | OpenCode's embedded server (its router in memory, no listener) with one plugin, in the drive's process. Nothing in the harness changes the process's environment (`process.env`): OpenCode's global paths come from the drive, not from XDG_* variables, which matters in a process that also runs libfx's agents (a new variable can crash libfx's native code, see [Beside libfx](#beside-libfx)); each drive's own global paths (home, config, data, cache, state, tmp) are a 0700 directory of its own under `root`, empty and removed when the drive ends, so OpenCode finds no skills, config or credentials of the machine. OpenCode has no execution plane on the machine (as in OpenCode's own workerd profile): its process spawner is its "unavailable" one, so nothing in it (git, formatters, ripgrep, MCP servers) starts a process; its filesystem service sees only the drive's own directories; snapshots and version control are no-ops; it reads no AGENTS.md or CLAUDE.md of the project or the machine's home, loads no plugin from config, and of its own provider and web-search plugins loads only the AI Gateway's (the others look for credentials and CLIs of the machine as they load, such as `az` on the PATH). Its working directory is `/workspace`, a path of the session's workspace: OpenCode refuses to start on a machine that has a real `/workspace` |
| The workspace | OpenCode's `@opencode/Environment` (`{ files, spawner }`), the seam every one of its built-ins reaches files and processes through, is the session's workspace (`environment.ts`): its files are the workspace's, and a command runs on the workspace's shell (`<shell> -c <script>` as a script there, OpenCode's ripgrep as the workspace's `rg`), with an environment built from a fixed allowlist, never this process's: a virtual workspace, or the session's Vercel Sandbox (`sandbox.ts`). The model is told the workspace's working directory and platform, not the machine's |
| The mirror | Every event OpenCode commits for the session, plus the instruction blobs its commit hooks write (they hold the date and the working directory), is shipped to the drive and journaled (`NativeJournal`, group-committed, one record per event) |
| The model barrier | OpenCode's `model.request` hook holds every model request (the turn's and compaction's) until everything committed so far is in the log, so a paid call never runs ahead of the log, and a drive that lost the session makes none |
| The tools | The app's `HostTool`s: each call run in the drive as `ctx.effect('tool:' + OpenCode's call id)` with the tool's replay policy; a `never` call waits for the journal first. With a `workspace`, OpenCode's own shell, read, write, edit, patch, glob and grep next to them (`builtins` narrows them), each call asking the drive before it runs (`tool.execute.before`: its intent committed, after the events that made the call are journaled, else the call fails `Not run`), running on the workspace, and reporting its outcome after (`tool.execute.after`). A call id an earlier step already used (some providers reuse ids, e.g. Kimi's `functions.bash:0`) is keyed `tool:<id>@<assistant message id>`, so a new call runs instead of answering from the old one's record. Every other tool of OpenCode's is removed, the session's permissions deny it (and any path outside `/workspace`), and a call to it is refused before it runs |
| Restore | On every drive: replay the journaled events into an empty database with OpenCode's own `Bus.replay` (its projectors rebuild every table), insert the project row and the blobs, and restore the execution claim and resume count, which live outside the events |
| Recovery | If a crash cut the last turn off: a recovery note built from the log (each tool call since the turn began: completed with its output, a `never` call "may have taken effect", a `safe` one "safe to repeat"), the claim, and OpenCode's own restart sweep, which settles the calls that were in flight as interrupted and continues the turn |
| The inbox | Each submission is placed, then admitted as OpenCode message `msg_<submission id>` (OpenCode dedupes by id, so a resend is a no-op). Steers are admitted mid-turn. A turn ends when OpenCode is idle, or when its execution fails (the transcript's `turn` entry carries OpenCode's error) |
| The credential | OpenCode's built-in AI Gateway provider (`vercel/<model>`) points at a loopback proxy in the drive, which adds the credential per request. The host holds only a placeholder: a fresh key per drive that the proxy checks, so a process without it can't use the proxy's credential. Embedded, OpenCode reads the process's environment and sends its `AI_GATEWAY_API_KEY` instead, when one is set, so the proxy accepts that key too |
| Live | Text deltas to `ctx.live` (`{ type: 'text', delta }`), the bridge's `tool_start` / `tool_end`, `{ type: 'idle' }` at a turn's end |
| The end of a drive | Once the drive is over, whatever the host still sends while it shuts down is dropped: no journal record, no live text, and its tool calls and model barriers are refused. A drive that is stopped stops its OpenCode runtime. The drive's directory goes when it ends; the next drive of the session on the machine removes what a dead process's drive left |

The normalized transcript (`harness-world-process`'s `transcript` stream) is
written next to the events, so the demo's view renders an OpenCode session
like any process harness.

## Use it

### The World and the sessions

```ts
// lib/world.ts: the only line that knows the backend
import type { World } from '@workflow/world'

export const world: World = process.env.VERCEL
  ? (await import('@workflow/world-vercel')).createWorld()
  : process.env.WORKFLOW_POSTGRES_URL
    ? (await import('@workflow/world-postgres')).createWorld()
    : (await import('@workflow/world-local')).createWorld({ dataDir: '.workflow-data' })
```

```ts
// lib/sessions.ts
import { durableSessions } from 'harness-world'
import { opencodeHarness } from 'harness-world-opencode'
import { world } from './world'

export const sessions = durableSessions({
  world,
  harness: opencodeHarness({
    model: 'anthropic/claude-sonnet-5',   // any AI Gateway model id
    // workspace: { kind: 'virtual', runtime: workspaceRuntime() }, // OpenCode's own tools on a virtual workspace
    // tools: [lookupCustomer],           // the app's tools
    // credential: () => Promise<string>  // default: AI_GATEWAY_API_KEY, else the Vercel OIDC token, per request
  }),
})
```

```ts
// app/.well-known/durable-harness/v1/flow/route.ts: the kernel's queue (__harness_wkf_workflow_*) is delivered here:
// world.registerRoute(QUEUE_PREFIX, '/.well-known/durable-harness/v1/flow') on world-local and world-postgres,
// a vercel.json queue trigger on __harness_wkf_workflow_* on Vercel
import { sessions } from '@/lib/sessions'
export const maxDuration = 800
export const POST = sessions.handler
```

```ts
// app/api/chat/route.ts
import { sessions } from '@/lib/sessions'

export async function POST(req: Request) {
  const { sessionId = (await sessions.start()).sessionId, text } = await req.json()
  const submissionId = await sessions.send(sessionId, text) // durable once this returns; the queue drives it
  return Response.json({ sessionId, submissionId })
}
```

```ts
// app/api/chat/[id]/live/route.ts: text deltas, tool starts and ends, idle
import { sessions } from '@/lib/sessions'

export async function GET(req: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params
  const body = new ReadableStream({
    async start(c) {
      for await (const ev of sessions.live(id, { signal: req.signal })) c.enqueue(`data: ${JSON.stringify(ev)}\n\n`)
      c.close()
    },
  })
  return new Response(body, { headers: { 'content-type': 'text/event-stream' } })
}
```

`sessions.inspect(id)` returns the fold; `transcriptOf(state)`
(`harness-world-process/records`) is the conversation, and `eventsOf(state)`
(`harness-world-opencode/records`) OpenCode's own events.

### Next.js

OpenCode reads files from its own package directories (migrations, assets,
native prebuilds): keep it out of the bundle, and depend on it from the app,
so the externals resolve from the app's `node_modules`:

```ts
// next.config.ts
transpilePackages: ['harness-world', 'harness-world-process', 'harness-world-opencode'], // they ship TypeScript
serverExternalPackages: ['@opencode/client', '@opencode/core', '@opencode/plugin', '@opencode/schema', '@opencode/server'],
```

```sh
pnpm add @opencode/client@2.0.22 @opencode/core@2.0.22 @opencode/plugin@2.0.22 @opencode/schema@2.0.22 @opencode/server@2.0.22
```

pnpm also asks about the build scripts of `@parcel/watcher`, `msgpackr-extract`,
`tree-sitter-bash` and `tree-sitter-powershell`; none is needed
(`allowBuilds: false` in `pnpm-workspace.yaml`). OpenCode's packages bring
`effect` 4.0.0-rc.112 themselves: don't install it, and if the app uses `effect`
too, pin it to that version. Do not add `effect` to
`serverExternalPackages`: Next optimizes it, and the two settings conflict.

### On Vercel

The same code. The host runs in the Function's own process: only `/tmp` is
writable there, so each drive's directory goes under it, and each drive
removes its directory when it ends. The credential is the
Function's OIDC token. OpenCode is traced into every Function that can drive
a session, which adds its JavaScript to each of them (a Function past the
size limit deploys as a
[Large Function](https://vercel.com/docs/functions/limitations#large-functions-beta)).
A Function is shared by every session its instance serves: OpenCode's own
tools reach only the session's workspace (a virtual one's shell runs in a
worker thread leased to that session, with an empty environment), never the
Function's disk, processes or environment, and the app's tools run in the
Function as code the app wrote.

### Beside libfx

One process can serve OpenCode's sessions and libfx's agents (with
`durable-harness/next`, one flow route drives both, in one Function).
libfx's native addon reads the process's environment through a pointer it
keeps: once the process has set any variable, a variable added later makes
libc move the environment and free the old copy, and libfx's next lookup
can crash the process. The harness never changes `process.env`, before or
after OpenCode is first imported (OpenCode computes default paths from
XDG_* and HOME as it loads; the harness uses none of them). The same goes
for the app: change `process.env` only before libfx loads, never while it
serves requests.

### Locally (world-local)

A script, without a flow route: hand the queue to world-local directly.
[`examples/local.ts`](examples/local.ts) is this, with live output:

```ts
import { createWorld } from '@workflow/world-local'
import { durableSessions, QUEUE_PREFIX } from 'harness-world'
import { workspaceRuntime } from 'harness-world-hands/runtime'
import { opencodeHarness } from 'harness-world-opencode'

const world = createWorld({ dataDir: '.workflow-data', recoverActiveRuns: true })
// OpenCode's own tools on a virtual workspace (durable-harness's `virtual()`, resolved).
const sessions = durableSessions({ world, harness: opencodeHarness({ workspace: { kind: 'virtual', runtime: workspaceRuntime() } }) })
world.registerHandler(QUEUE_PREFIX, sessions.handler)
await world.start()

const { sessionId } = await sessions.start()
await sessions.send(sessionId, 'Write a haiku to haiku.txt, then read it back.')
```

```sh
pnpm --filter harness-world-opencode example "Write a haiku about durable logs to haiku.txt"
```

The workspace's files are in the run's log (`filesFromState`). `kill -9` the
script mid-turn and run it again with `--resume <sessionId>`: the start
re-enqueues the run, and the cut-off turn finishes from the log.
In a Next app, start the queue once per process:

```ts
// instrumentation.ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== 'nodejs' || process.env.VERCEL) return
  const { world } = await import('./lib/world')
  await world.start?.()
}
```

### Postgres (world-postgres)

The same code and the same `instrumentation.ts`, with `WORKFLOW_POSTGRES_URL`
set and the World's migrations run once (`node
node_modules/@workflow/world-postgres/bin/setup.js`). world-postgres delivers
its queue over HTTP to the app's flow route, so a session's drives can land
on any instance: the log carries the session.
`test/e2e.test.ts` with `WORKFLOW_POSTGRES_URL` runs on it (a turn with a
tool; a crash in `unsafe_make_payment` recovered by another process, one
payment). The demo stopped in the middle of the payment with no crash record
(chaos `silent: true`) was recovered by the queue after its restart: the call
failed as interrupted, one ledger record, the turn completed.

### In a browser tab

Not as a runtime: OpenCode needs Node (its SQLite, its server). A tab can watch a session a server drives: open the server's World
through `harness-world-remote` (`createRemoteWorld({ baseUrl: '/api/world',
queue: createLocalQueue() })`, served by `createWorldHandler`), then read the
log with `inspect` and `live` and render it with `transcriptOf`. Send
messages through a server route, not from the tab: the tab's queue is its
own.

## Tools

**Tool-less by default.** Without a `workspace`, the app's tools
(`tools`, `HostTool[]`) are the session's only tools: OpenCode's own are
removed from the request, the session's permissions deny them, and a call
to one is refused before it runs (`No tool named … is available`).
`builtins` without a `workspace` is a `TypeError`.

**With a `workspace`, OpenCode's own tools run on it**: shell, read, write,
edit, patch, glob and grep (`OPENCODE_TOOLS`; `builtins: true` says so, a
list narrows them), with OpenCode's own names, schemas and code. The rest of
OpenCode's tools (webfetch, websearch, skill, questions, sub-agents, code
mode) stay off.

- **On the workspace, never on the machine.** OpenCode's files and
  commands go through its Environment seam to the workspace: a virtual
  workspace's files are the session's (in the log, or in memory for one
  turn), and its shell is a bash-compatible shell over them, with no
  network and no real binaries. A command's environment is a fixed
  allowlist. The read tool's view of a path outside `/workspace` is denied
  (`external_directory`), and so is every other action.
- **What the model is told.** Each built-in's own description, with what
  does not hold on a workspace edited out (the host's platform and shell,
  output saved to a file, background commands) and the workspace's note
  added (where it runs; how long its files last). shell's `background`
  parameter is removed: a command ends with its call. The system prompt
  names `/workspace` as the working directory, and what the workspace is.
- **Every call is in the log before it runs.** The plugin's
  `tool.execute.before` hook ships what OpenCode committed (its record of
  the call is in the log first), then asks the drive, which commits the
  call's intent (`tool.<name>`, keyed `tool:<call id>`) and answers; a call
  whose intent cannot be committed fails before it runs (`Not run`).
  `tool.execute.after` commits the outcome.
- **Its changes commit with its outcome.** On a durable virtual workspace
  each call runs on the files as the log has them, and what it changed is
  committed with its outcome, or not at all: a call cut off by a crash
  changed nothing, so every built-in is `safe` to repeat. A call whose
  changes cannot be kept (the workspace is full) reports that failure to
  the model instead of OpenCode's result. In memory
  (`virtual({ durable: false })`), the files last the turn, and a turn
  picked up by another drive starts with an empty workspace, which the
  first call says.
- **On a sandbox** (`sandbox()`, resolved: `{ kind: 'sandbox', acquire }`,
  the sandbox tier's `AcquireMachine`): OpenCode's core stays in the drive,
  and each call's files and commands go to the session's own VM, its
  `/workspace` on the session's Drive, as the VM's unprivileged user with
  the relay's environment allowlist (`sandbox.ts`). The VM is attached on
  the first built-in call of a drive (a turn that runs none boots none),
  through the kernel's `attachInDrive`: a `sandbox.attach` fact armed with
  the drive's own fencing token, the `hw.sandbox` record. The attach comes
  before the call's intent: one that fails or is busy makes the call
  `Not run: the sandbox is starting, try again`, a definite outcome, and one
  that stops the drive (preempted, the VM-time budget spent) stops it. The
  Drive keeps what a call did, even one a crash cut off, so write, edit,
  patch and shell are `never` replayed (read, glob, grep `safe`), and each
  write is flushed to the Drive before its outcome. The files last across
  turns and VMs; when the Drive itself was lost, the first call after says
  so. Calls run one at a time. glob and grep need `rg` in the VM (the base
  has Codex's).

## What a crash costs

- **A model call in flight.** Lost and billed again: OpenCode's stream is not
  a step, and its text deltas are never stored. The resumed turn regenerates
  it. (Around a "Crash during: model" drill the drive writes a record-only
  `model` step for each request it proxies, `ModelSteps` in
  harness-world-process, so the crash has a step to fire in; the recovered
  turn's first request retries it. Without a drill armed, none is written.)
- **The host.** It goes with the drive's process; its directory is removed
  by the next drive of the session on the machine.
- **A tool call in flight.** Never run twice by the harness: a `never` tool
  comes back interrupted ("may have taken effect"), a `safe` one is reported
  safe to repeat, and OpenCode settles the call itself as `Tool execution
  interrupted`. On a durable virtual workspace a cut-off built-in changed
  nothing (its changes commit only with its outcome); on a sandbox it may
  have partly run (what it wrote before the cut is on the Drive), and it is
  reported as not safe to repeat. The model may decide to call it again, as
  a new call; the note tells it to check first.
- **Committed events not yet journaled.** Lost: at most the work since the
  last barrier or shipment (every model request and tool call is a barrier),
  never a tool's intent.
- **Recovery delay.** The kernel's: at once when a chaos crash recorded the
  death or the dead process's pid is gone, else the queue's redelivery of
  the dead delivery (no lease of the kernel's on top).
- **One recovery turn**, and the restore: every drive replays the session's
  events (it grows with the session).
- **A budget boundary is a crash too.** A drive that runs out of time (a
  Function near `maxDuration`) stops its host, and the next drive resumes
  the turn with a note. OpenCode resumes one turn a bounded number of times
  (its resume count is restored from the log on every machine), so past
  that the turn fails ("Execution was interrupted repeatedly").

## Records in the log

| Stream (journal) | Record id | What |
| --- | --- | --- |
| `opencode.events` | `seq:<n>` | One committed OpenCode event: `{ id, seq, created, type, data }`, as OpenCode's `event` table stores it and `Bus.replay` takes it |
| `opencode.blobs` | `blob:<hash>` | One instruction blob `{ hash, value }` |
| `transcript` | | The normalized transcript (`harness-world-process`) |

Tool calls are effects `tool:<OpenCode call id>` (step `tool.<name>`;
`tool:<id>@<assistant message id>` for an id an earlier step used, and the
transcript names the call the same way); placements are
`place:<submission id>`. A durable virtual workspace's files are the
session's `hw.workspace` document, each call's changes committed with its
outcome.

## Options

| Option | Default | |
| --- | --- | --- |
| `tools` | none | The app's tools (`HostTool[]`); without a `workspace`, the session's only ones |
| `workspace` | none | Where OpenCode's own tools run, resolved: `{ kind: 'virtual', runtime }` (a `harness-world-hands/runtime` workspace runtime) or `{ kind: 'sandbox', acquire }` (the sandbox tier's `AcquireMachine`). durable-harness resolves `virtual()` and `sandbox()` into them |
| `builtins` | all seven with a `workspace` | `true`, or a list of `OPENCODE_TOOLS` to narrow them; without a `workspace`, a `TypeError` |
| `model` | `anthropic/claude-haiku-4.5` | An AI Gateway model id (OpenCode's `vercel/…` form works too); it must be in OpenCode 2.0.22's bundled model catalog (`models.fetch` is off) |
| `root` | `<os temp dir>/harness-opencode` | Where each drive keeps OpenCode's own files (its database, rebuilt from the log; its empty config and global directories), a directory per drive removed when it ends |
| `credential` | `gatewayCredential` | The Gateway credential, read per request |
| `gatewayUrl` | `AI_GATEWAY_BASE_URL` or `https://ai-gateway.vercel.sh` | |
| `instructions` | `DEFAULT_INSTRUCTIONS` without a workspace | Added to every model request's system prompt, before what the workspace adds |
| `title` | `harness-world session` | The session's title (OpenCode's title model call is skipped) |
| `name` | `opencode` | The kernel name (`harness:opencode`) |

`mode`, `directory`, `host` and `workspaceNote` are gone (each a `TypeError`
naming what replaces it). `HARNESS_OPENCODE_DEBUG=1` traces the protocol between the drive and the
host, and OpenCode's info logs, on stderr.

## Tests

`pnpm --filter harness-world-opencode test`. Without a Gateway credential
every file but `test/e2e.test.ts` runs (no network):

| File | What |
| --- | --- |
| `src/records.test.ts` | The readers (status, transcript, blobs) on events recorded from real turns (`test/fixtures/`) |
| `src/host.test.ts` | Real OpenCode, no network: rebuilding a finished session and a crashed one (claim, resume count) from the journal alone; the outbox and the barrier on a new session, and a refused barrier making no model call |
| `src/gateway.test.ts` | Real OpenCode against a stand-in Gateway: the model request arrives with the drive's credential, with and without an `AI_GATEWAY_API_KEY` in the process's environment, and the proxy refuses a request that carries neither key |
| `src/harness.test.ts` | The drive against a scripted host on world-local: the mirror, tool calls on the bridge keyed by OpenCode's call id (and a reused id), the barrier, a steer, a crash during `unsafe_make_payment` and its recovery note, a "Crash during: model" drill after a `never` tool (the scripted host's model requests go through the drive's proxy to harness-world-process's `fakeModel()`), a refused journal write stopping the host, a drive that loses the race to start a host leaving the session's files alone, nothing reaching the log after a drive is over; with the built-ins on a virtual workspace: a call's intent before it runs (after OpenCode's event for it) and its outcome after, a crash mid built-in resumed with a note that it is safe to repeat, and an in-memory workspace reset when a new drive picks the turn up; the options (`builtins` without a workspace, an unresolved workspace, the removed options) |
| `test/boot-and-tools.test.ts` | Real OpenCode against a scripted model, on a virtual workspace, from a directory with a canary AGENTS.md, a planted `~/.claude/CLAUDE.md`, skills and OpenCode's own global AGENTS.md, canary values in the environment, and this process's `node:fs` and `node:child_process` calls traced: OpenCode's write, read, edit, grep, glob and shell work on `/workspace`; a command's environment is the virtual shell's own and the allowlist; paths outside the workspace are denied (`external_directory`); no canary or path of the machine reaches the model; no process starts; nothing searches the machine's PATH; nothing outside the drive's own directories is touched; no `/workspace` on the machine before or after, and OpenCode refuses to start where there is one; in memory, nothing in the log and an empty next turn; a call whose changes cannot be kept; `builtins` narrowing; a crash mid-tool (SIGKILL of the app's process) resumed by a fresh process from the log, the cut-off call having changed nothing |
| `test/sandbox.test.ts` | `test/boot-and-tools.test.ts`'s suite on a sandbox, through the sandbox tier's real session factory and the kernel's `attachInDrive`, on its stand-in (always: harness-sandbox's fake Sandbox API, each VM a local relay over temp dirs, each Drive a temp dir, `test/stand-in.ts`): OpenCode's write, read, edit, grep, glob and shell in the session's VM, attached at the first built-in call, the calls that change files or run commands `never` and the rest `safe`, the files on the Drive and none in the log; a command's environment is the relay's allowlist and the harness's, never this process's; `external_directory` denied; no canary or path of the machine reaches the model; this process starts nothing but the stand-in's VM and touches nothing outside the drive's own directories; a turn with no built-in call boots no VM; files written on VM A read on VM B after VM A is deleted; a lost Drive told once and acknowledged; an attach that fails making the call "not run", the next one attaching; `builtins` narrowing; OpenCode refusing to start with a host `/workspace`; a crash mid-command (SIGKILL of the app's process) resumed by a fresh process on a new VM over the same Drive, the command never rerun and reported as possibly partly run, what it wrote before the cut kept. With `DH_LIVE_SANDBOX=1`, from a directory linked to a Vercel project: the same on a live Vercel Sandbox (VM A, deleted, VM B) |
| `src/versions.test.ts`, `src/versions-drive.test.ts` | The pins equal package.json's; another installed OpenCode or `effect` fails the turn before OpenCode starts, with both versions and the install line, and a fixed install drives the next message |
| `test/tool-less.test.ts` | Real OpenCode against a scripted model, with no workspace, from a directory with a canary AGENTS.md, a planted `~/.claude/CLAUDE.md` and skills, canary values in the environment, and this process's `node:fs` and `node:child_process` calls traced: only the app's tools are offered, calls to OpenCode's shell, read and write are refused before they run, no canary and no path of the machine reaches the model, no process starts, nothing searches the machine's PATH, nothing is read where the canaries are and nothing is written outside the session's own directories |
| `test/beside-libfx.test.ts` | A real libfx engine and OpenCode's drives in one process (`test/beside-libfx.ts`, a process of its own): a libfx turn, then a drive, ten times, with freed memory reused in between; the process exits cleanly and its environment never changes once libfx runs |
| `test/e2e.test.ts` | With `AI_GATEWAY_API_KEY` or `VERCEL_OIDC_TOKEN`: a real turn with a tool call and a second turn on an empty root; a steer while a tool runs; a chaos crash (SIGKILL) of the app's process during `unsafe_make_payment`, recovered in a fresh process on an empty root (one payment in the ledger); a SIGKILL while the model streams, recovered on the same root (the dead drive's files swept); two "Crash during: model" drills (the app killed by its own chaos crash in a model request, armed before the prompt and as `unsafe_send_email` starts; the killed `model` step retried on recovery, one email); OpenCode's own write, shell, edit and read on a virtual workspace, then a turn on another root reading the file from the log; a chaos crash in the middle of a built-in shell command, recovered in a fresh process (the call cut off with everything it changed, reported safe to repeat, never rerun by itself); and, with `WORKFLOW_POSTGRES_URL` (migrations run), a turn and a chaos crash during `unsafe_make_payment` on world-postgres |

## Not done yet

- **Subagents, questions and code mode** stay off; a subagent's state lives
  outside the session's events.
- **Concurrent tool calls** in one step run one at a time (the bridge
  serializes them).
- **Restore cost** grows with the session (every drive replays every event);
  a checkpoint of the SQLite file would bound it.
- **One process, many sessions**: each concurrent drive holds an OpenCode
  runtime, which is large. Drives of different sessions run side by side; a
  second drive of the same session in one process yields (`contended`).
- **Upstream internals**: restore depends on `Bus.replay`, the `session_v2`
  columns behind the claim and the resume count, and the instruction-blob
  table. Re-run the tests before moving off 2.0.22.

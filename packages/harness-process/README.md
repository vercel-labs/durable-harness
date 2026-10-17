# harness-world-process

The shared core of **process harnesses** on harness-world: agent harnesses
whose loop runs in a child process the adapter does not control, such as
Claude Code, Codex and OpenCode v2. A private workspace package, bundled
into [`durable-harness`](../durable-harness).

Each port is a kernel `Harness` registered with `durableSessions()`, like Pi
and fx, so every one of them gets the same user DX (`sessions.start`,
`send`, `live`, `handler`), the kernel's driver, its queue-anchored
recovery (no heartbeats: a dead child's drive is recovered at once, its pid
gone) and fencing, and the demo's UI:

```ts
const sessions = durableSessions({ world, harness: claudeCodeHarness({ tools: workspaceTools() }) }); // the app's tools, each call in the log
export const POST = sessions.handler;
```

What this package gives a port:

| Piece | What it does |
| --- | --- |
| `NativeJournal` | Mirrors the harness's native session records into the run's log, group-committed, deduped by record id; a `flush()` barrier; large records offloaded to a hash-checked blob stream |
| The normalized transcript | One stream every process harness also writes (assistant text, tool calls, turn ends), so one view renders any of them |
| `ProcessHost` | One child per session in its own process group, an explicit environment, a per-session directory, a pidfile, kill-before-resume of a stale child, a registry of the process groups its tools' shells start (killed with it), a watchdog |
| `toolBridge` | `HostTool`s as a harness-neutral tool list; every call is `ctx.effect('tool:' + callId)` with the tool's replay policy |
| `BuiltinCalls` | The harness's own tools (Bash, shell, edit, ...) in the log: an intent committed from the harness's pre-call hook before the call runs, the outcome from its post-call hook; never rerun |
| `openWorkspace`, `workspaceNote` | Which copy of the working directory a drive used, in the log; the note a drive on another copy tells the model |
| `recoveryNote` | The user message a resumed turn starts with, built from the log |
| `placeSubmissions`, `waitForSubmissions` | Place-before-send for the inbox |
| `gatewayCredential`, `gatewayProxy`, `gatewayToken` | The AI Gateway credential, fresh per call, a loopback proxy that adds it, so the child never holds it, and the per-drive key the proxy checks |
| `ModelSteps` | A record-only `model` step around the child's model requests (seen at the proxy), written only around a "Crash during: model" drill, so the drill has a step to fire in |
| `FileTail`, `zodFromJsonSchema` | A line-complete file tail; JSON Schema to zod for SDKs whose tools take zod |

`reference/` holds a fake CLI (`fake-cli.mjs`) and the reference harness
that drives it (`reference/harness.ts`), built only from these pieces. It is
the worked example: read it before writing a port. Its tests
(`reference/reference.test.ts`) run a turn with a tool, a crash in the
middle of a `replay: 'never'` tool recovered on a fresh machine and on the
same one, a refused journal write, and (with the CLI's replies streamed from
`fakeModel()` through the drive's gateway proxy) a "Crash during: model"
drill after a `never` tool, all on world-local with a real child process.
`reference/fake-model.ts` (`harness-world-process/reference/fake-model`) is
that stand-in model, for any port's scripted tests.

## The shape of a drive

The bridged `HostTool`s (the app's, or `harness-world-hands`'s log-backed
workspace tools) are a harness's only tools by default (Claude Code, Codex
and OpenCode). With `builtins: true`, a harness's own tools (Claude Code's,
Codex's shell and `apply_patch`; OpenCode's, given a `workspace`) sit next
to them, each call recorded through the harness's hooks (`BuiltinCalls`):
its intent is in the log before it runs, its outcome after, and nothing
reruns one after a crash. Either way every call the harness makes has an
intent in the log before it runs. What differs is the workspace: a CLI
built-in's working directory is a disk (the session's Drive, in its Vercel
Sandbox), not the log (see [Built-in tools](#built-in-tools)); OpenCode's
run on its virtual workspace inside `BuiltinCalls`' `around` hook, which
commits a durable workspace's changes with the call's outcome.
Every drive:

1. Returns at once when there is nothing to do (`recoveryNote()` is
   undefined and `ctx.pending()` is empty): an idle wake starts no child.
2. Spawns the harness (`host.spawn`): it kills a child a dead drive left
   running, then the harness's `prepare` rebuilds its native session from
   the journal (the log wins: a local copy that differs is overwritten),
   then the child starts, with `signal: ctx.signal` so a stopped drive kills
   it.
3. Mirrors every native record the child writes into the journal.
4. If the last turn never ended (a crash cut it off), sends the recovery
   note first.
5. Places each submission, then sends it. Serves the harness's tool calls
   through the bridge, and records its built-in calls through its hooks; a
   `never` call waits for the journal (`beforeNever: () => journal.flush()`).
6. Writes the normalized transcript and live events (`text`, `tool_start`,
   `tool_end`, `idle`). At each turn's end: a `turn` entry and a flush.
7. Closes the child and returns. The kernel drives again if submissions
   arrived meanwhile.

A port maps this onto its harness: Claude Code's journal is its
`SessionStore.append` (SDK in-process), Codex's is its rollout file
(`FileTail`), and OpenCode's are its committed events (outbox). The bridge
becomes Claude Code's in-process MCP server, Codex's dynamic tools, or
OpenCode's plugin tools.

## Example

A harness for a CLI that keeps a JSONL session file and speaks JSON lines
on stdin/stdout (`reference/harness.ts` is the full version, with the
`ready` handshake, child-exit errors and cleanup; steering is left out):

```ts
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Harness, HostTool, Submission } from 'harness-world';
import { FileTail, NativeJournal, placeSubmissions, ProcessHost, recoveryNote, toolBridge } from 'harness-world-process';

export function myCliHarness(tools: HostTool[], host = new ProcessHost()): Harness {
  return {
    name: 'my-cli',
    async drive(ctx) {
      if (!recoveryNote(ctx.state()) && ctx.pending().length === 0) return; // an idle wake: no child
      const journal = new NativeJournal(ctx);
      const file = join((await host.dir(ctx.runId)).path, 'session.jsonl');
      const text = (await journal.read<string>('native')).map((r) => `${r.data}\n`).join('');
      const tail = new FileTail(file, Buffer.byteLength(text));
      const cli = await host.spawn(ctx.runId, {
        command: 'my-cli',
        args: ['--session', file],
        signal: ctx.signal, // a stopped drive kills the child's group
        prepare: () => writeFileSync(file, text), // the log wins; a stale child is dead by now
      });
      const bridge = toolBridge(ctx, tools, { beforeNever: () => journal.flush() });
      const events = cli.lines()[Symbol.asyncIterator]();
      const turn = async (text: string, placed: Submission[]) => {
        await placeSubmissions(ctx, placed); // place, then send
        cli.send({ type: 'user', text });
        let said = '';
        for (;;) {
          const ev = JSON.parse((await events.next()).value);
          for (const line of tail.read()) journal.append('native', line);
          if (ev.type === 'text') {
            said += ev.delta;
            ctx.live({ type: 'text', delta: ev.delta }); // the live-event vocabulary the demo reads
          }
          if (ev.type === 'tool_call') {
            journal.message({ type: 'message', role: 'assistant', text: said, toolCalls: [{ id: ev.id, name: ev.name }] });
            said = '';
            cli.send({ type: 'tool_result', id: ev.id, ...(await bridge.call(ev.name, ev.args, ev.id)) });
          }
          if (ev.type === 'turn_end') {
            if (said) journal.message({ type: 'message', role: 'assistant', text: said });
            journal.message({ type: 'turn', status: 'completed' });
            return journal.flush();
          }
        }
      };
      const note = recoveryNote(ctx.state()); // a turn a crash cut off
      if (note) await turn(note.text, note.pending);
      for (let s = ctx.pending()[0]; s; await ctx.refresh(), s = ctx.pending()[0]) await turn(s.text ?? '', [s]);
      cli.end();
    },
  };
}
```

## API

### `NativeJournal`

```ts
const journal = new NativeJournal(ctx, { inlineMaxBytes?, onPoison? });
journal.append(stream, data, { id? }): boolean   // queue a JSON value; false if `id` is already in, or poisoned
journal.message(entry: TranscriptEntry): boolean // append to the normalized transcript
await journal.flush()                            // everything appended so far is committed
journal.records(stream?)                          // sync, from ctx.state(); offloaded records have data: undefined
await journal.read(stream?)                       // the same, blobs read back and hash-checked
journal.length(stream), journal.has(id), journal.poisoned
```

- **Records in the log.** A batch is one record-only effect `journal:<n>`
  (step name `journal`) whose input is `{ v: 1, n, streams: { [stream]: {
  from, entries } } }`. Two events per batch (its intent and its outcome).
  The kernel makes batches linear: the first committed step per key wins, a
  stale driver's step is fenced, and a batch counts only once its outcome is
  committed (a writer that died between the two writes loses that batch,
  and the next writer reuses its key). Every reader, the writer included,
  sees the same records in the same order.
- **Group commit.** One batch in flight; everything appended while it is
  written goes in the next one. A synchronous burst of appends is one batch.
- **Streams** are names the harness picks (`native`, `claude:main`,
  `rollout`, `opencode.events`, ...). Positions (`seq`) are per stream.
- **Dedupe.** A record with an `id` (Claude Code's entry uuid, OpenCode's
  event seq, a Cursor blob id) is appended once per session, whichever drive
  appends it.
- **The barrier.** `flush()` before a `never` tool runs (the bridge's
  `beforeNever`) and at turn end. With run-ahead durability
  (`DriverOptions.runAhead`) a batch is issued, not yet durable, when its
  effect returns; the kernel waits for it at its own barriers (before a
  `never` effect, before the drive goes idle), so `flush()` before a `never`
  tool still means "in the log before the tool".
- **Offload.** A record whose JSON text is over `inlineMaxBytes` (32 KiB)
  goes to the session's blob stream `<runId>_blobs` as `{ h: sha256, s: text
  }` (split into 2M-character pieces), written before its batch commits;
  the batch keeps `{ blob, bytes }`. Reads trust a blob only for the hash it
  is named by; a blob the log names and the stream does not hold is `Fatal`.
  The transcript stream is never offloaded.
- **Poison.** A batch write that fails or is refused poisons the journal:
  `append` becomes a no-op, `flush` rejects, and `onPoison(err)` runs
  (default `ctx.halt(err)`, which stops the drive and so kills the child).
  These are in-band writes of the drive (they go through its `SessionLog`
  via `ctx.effect`), never `hook_received`.
- **Data** must be JSON. Binary native state goes in base64.

`journalBatches(state)`, `journalRecords(state, stream?)` and
`transcriptOf(state)` are the same reader over any fold, in
`harness-world-process/records` (no Node imports: UIs use it).

### The normalized transcript

Every process harness writes the `transcript` stream next to its native
ones, through `journal.message(entry)`:

```ts
type TranscriptEntry =
  | { type: 'message'; role: 'assistant'; text: string; toolCalls?: { id: string; name: string }[] }
  | { type: 'message'; role: 'user'; text: string; synthetic: string } // a message the harness sent itself ('recovery')
  | { type: 'turn'; status: 'completed' | 'failed' | 'interrupted'; error?: string };
```

- User prompts are not in it: they show where their placement is
  (`placeSubmissions`).
- A tool call's input and result are its tool effect (`tool:<id>`), not
  copied here.
- A `turn` entry ends a turn. `openTurn(state)` is everything after the last
  one; the demo shows a session as busy, and streams its live text into a
  row of its own, while a turn is open.

### `placeSubmissions(ctx, submissions)` and `waitForSubmissions(ctx)`

Place before send: one record-only effect `place:<first id>` with `placed`.
Placing takes a submission off `ctx.pending()`; a withdraw committed first
wins (the effect throws `Restart`, and the drive rebuilds without it). A
drive that leaves a submission unplaced is driven again, so place every
submission the harness takes. `waitForSubmissions` resolves with the
pending submissions as soon as there are any (hints, else a poll), or `[]`
when the drive stops: for a harness that keeps its child up for steers.

### `toolBridge(ctx, tools, options)`

```ts
const bridge = toolBridge(ctx, tools, { beforeNever, serial = true, live = true });
bridge.tools // [{ name, description, inputSchema, replay, call(args, callId, signal?) }]
await bridge.call(name, args, callId, signal?) // { text, isError, interrupted? }
```

- Key `tool:<callId>`, step name `tool.<name>`, the tool's `replay`,
  `requires` and `needs`. The call id must be the harness's own and stable
  across a resume of its session: it is the memo key, so a call the log
  already finished answers from the log without running.
- A committed failure is an error result. An interrupted `never` call is an
  error result saying it may or may not have taken effect, with its last
  progress (`failureText`).
- Anything else the effect throws (the drive was preempted, a recovery
  resolved the call first, a write failed) halts the drive and is rethrown:
  the harness is never handed a result the log does not have.
- Calls are serialized (a per-drive mutex); concurrent effects in one drive
  are not exercised by the kernel yet.
- `zodFromJsonSchema(schema, z)` / `zodShapeFromJsonSchema(schema, z)` turn
  an `inputSchema` into zod for the SDKs that want it (Claude Code's
  in-process MCP tools, OpenCode's plugin tools). Pass the harness SDK's own
  `z`.

### Built-in tools

```ts
const builtins = new BuiltinCalls(ctx, { replay?, beforeNever?, live = true, outputChars = 8000, around? });
const start = await builtins.begin({ callId, name, input, replay? }); // in the harness's pre-call hook
if (!start.run) /* refuse the call: */ start.result;                   // the log already has its outcome
await builtins.end(callId, { text, isError? });                       // in its post-call hook
await builtins.interruptAll();                                         // the harness died with calls open
```

- **Intent before the call.** `begin` commits a born-running step
  `tool.<name>`, key `tool:<callId>` (the same key space as the bridge), and
  resolves once it is committed; a `never` call (the default) first runs
  `beforeNever` (the native journal's barrier, so the harness's own record
  of the call is in the log before its intent) and, with run-ahead
  durability, waits for durability. The hook lets the call run only then,
  and refuses it if `begin` rejects (fail closed).
- **Outcome after.** `end` commits the result (clipped to `outputChars`:
  the full result is in the harness's own transcript) or, with `isError`, a
  failed step carrying the text.
- **Around the call.** `around(call, run, io)` runs each call's effect body
  inside a wrapper, for built-ins that work on files the drive keeps
  (OpenCode's, on a virtual workspace): it syncs the files in before `run`
  and commits what the call changed with its outcome (`io.docs`). What it
  returns is the recorded outcome; one that throws before `run`, or returns
  without it, decides the call, which never runs.
- **Never rerun.** A call id the log already finished makes `begin` answer
  `{ run: false, result }`: refuse it with that result. After a crash, a
  call that was in flight is resolved by the kernel like any intent whose
  holder died (`never`: failed, interrupted; `safe`: released), the harness
  answers it "interrupted" in its own session (Claude Code and OpenCode do
  it themselves; Codex gets it injected), and the recovery note reports it.
  For a built-in, `safe` changes only the note's wording ("safe to
  repeat"); the model decides.
- **The working directory is not the log.** `openWorkspace(journal, { path,
  marker })` records which copy of the directory a drive uses (a marker file
  the host keeps next to it; a journal record on stream `workspace` when the
  copy changes) and says whether it `moved`: another machine, a fresh
  `/tmp`, or an older copy. `workspaceNote(path)` is the `[Environment]`
  note a drive then sends the model once, before its first message.
- **Processes do not outlive the drive.** `ProcessHost` gives every child
  `BASH_ENV`, `ENV`, `SHELL` (bash where there is one) and
  `HARNESS_PROCESS_GROUPS`, and writes the session home's `.bashrc`,
  `.bash_profile`, `.profile` and `.zshenv`: every bash or zsh the child's
  tools start registers its process group there (`GROUP_REGISTRY_SCRIPT`;
  by pid where a sandbox refuses `ps`, as Codex's macOS one does). A tool's
  shell runs in a group of its own, which a kill of the child's group does
  not reach; the host kills the registered groups with the child (`kill`,
  a clean exit, `killStale` before the next resume, `killGroups`), and the
  watchdog (`spawn({ watchdog: true })`) does it within about a second of the
  drive's process dying. A registered group is killed only while it is
  still the session's (its leader's environment on Linux, its start time
  elsewhere). A plain `sh -c` reads no startup file and registers nothing:
  it stays in whatever group started it.

### `recoveryNote(state, options)`

```ts
const note = recoveryNote(ctx.state(), { pending?, prompts?, clipChars? });
// undefined when no turn is open; else { text, calls, pending, turn }
```

The calls since the turn began, each with what the log knows: completed
(with its clipped output), failed (its error), `interrupted` (a `never`
call: "may have taken effect; check before repeating"), `retryable` (a
`safe` call: "safe to repeat"). Then the pending submissions, which the note
carries (`note.pending`: place them with it). `prompts: true` quotes the
turn's own prompts, for a harness whose native session may have lost them.
Build it inside a drive: the kernel has resolved abandoned intents by then.

### `ProcessHost`

```ts
const host = new ProcessHost({ root? });          // default: HARNESS_PROCESS_ROOT, else /tmp/harness-sessions on Vercel, else ./.sessions
await host.dir(runId)                              // { path, home, tmp, state }: <root>/<runId>/
const child = await host.spawn(runId, { command, args, env, cwd?, signal, prepare?, stderr? });
child.lines(), child.send(value), child.end(), child.alive(), child.kill(), child.adopt(pgid), child.exited, child.stderrTail()
await host.killStale(runId), host.reset(runId), host.pidRecord(runId)
```

- **Own process group** (`detached`): a kill reaches everything the child
  starts that stays in the group, and a SIGKILL of the parent does not reach
  the child. `child.adopt(pgid)` records a group a tool left (setsid) so it
  is killed too; the group registry (see [Built-in tools](#built-in-tools))
  does the same for every shell its tools start.
- **`watchdog: true`**: a `sh` loop in a group of its own kills the child's
  group and the registered ones within about a second of this process
  dying, and leaves once there is nothing left to kill.
- **Explicit environment.** `PATH`, `HOME` (the session's `home/`), `TMPDIR`
  (its `tmp/`), `SHELL`, `BASH_ENV` and `ENV` (the group registry), what you
  pass, and `HARNESS_PROCESS_SESSION`, `HARNESS_PROCESS_TOKEN` and
  `HARNESS_PROCESS_GROUPS`. Nothing else of the parent's environment is
  inherited, so no secret reaches the child unless passed. On Vercel
  Functions only `/tmp` is writable and `HOME` may be read-only, which is
  why the default root is under `os.tmpdir()` there.
- **Kill-before-resume.** The pidfile (`<dir>/.harness/pid.json`) records the
  child. The next drive's `killStale` (also run by `spawn`) SIGKILLs its
  group, after checking the pid is still that child (the token in
  `/proc/<pid>/environ` on Linux; the command line and start time from `ps`
  elsewhere), and waits for it to go. A clean exit removes the pidfile.
- **`prepare(dir)`** runs with the session claimed, after the stale child is
  dead and before the new one starts: rebuild native session files there.
- **Overlapping deliveries.** If a drive of the same `ProcessHost` runs (or
  is starting) the session's child, `spawn` / `killStale` throw
  `Yield('contended')` instead of killing it (the delivery is retried in a
  moment); the kernel settles which delivery owns the session. Use one
  `ProcessHost` per process and root.
- `signal: ctx.signal` kills the group when the drive stops (budget,
  preemption, `ctx.halt`).
- POSIX only.

### Gateway credentials

- `gatewayCredential()`: `AI_GATEWAY_API_KEY`, else the Vercel OIDC token
  (`@vercel/oidc`), read fresh on every call.
- `gatewayProxy({ token, credential?, target?, headers? })` starts a
  passthrough proxy on `127.0.0.1` that forwards to the Gateway
  (`AI_GATEWAY_BASE_URL`, default `https://ai-gateway.vercel.sh`) and sets
  `authorization: Bearer` and `x-api-key` from a fresh credential per
  request, replacing the key the child sent. Responses stream through
  (SSE). `token` is required: pass a fresh `gatewayToken()` and give the
  child `proxy.url` as its base URL and the token as its key
  (`ANTHROPIC_BASE_URL` + `ANTHROPIC_AUTH_TOKEN`, an OpenAI-compatible
  `base_url` + key). The proxy forwards only requests that carry it
  (`authorization: Bearer` or `x-api-key`) and answers anything else 401:
  any process on the machine can reach a loopback port. `token` may also be
  a set of keys, any of which passes (embedded OpenCode: the key it was
  given, and `AI_GATEWAY_API_KEY` from the process environment, which it
  sends instead when that is set). The check stops processes that don't
  hold the child's key; a process of the same uid can read it from the
  child's environment (`/proc/<pid>/environ`), so a CLI's own tools belong
  only where the CLI is alone in its sandbox, and the VM path replaces the
  proxy with a relay. Verified against the Gateway with an OIDC token for
  `/v1/chat/completions` and `/v1/messages`.
- `track(request)` (an option of `gatewayProxy`) is told when each forwarded
  request comes in, and the function it returns when that request is over
  (its response ended, failed, or its connection closed). `ModelSteps` uses
  it.

### `ModelSteps`

```ts
const steps = new ModelSteps(ctx, { log? });
const token = gatewayToken(); // the child's key
const proxy = await gatewayProxy({ credential, token, track: steps.request, captureRequest: ctx.tracing !== undefined });
// ... the drive ...
await steps.close(); // before the drive returns, then proxy.close()
```

The kernel's chaos hook kills a drive from inside a step, and a process
harness's model call is its child's, never a step, so a crash armed for the
model ("Crash during: model") had nothing to fire in. The proxy sees every
model request (a POST, not a token count) start and end, and `ModelSteps`
writes a record-only effect around it, step name `model`, key
`model:<uuid>`, `replay: 'safe'`, input `{ method, path }`, output `{
requests, status | error }`:

- **Only around a drill.** A step opens only while a process crash that
  targets the model (`at: 'model'` or `'any'`) is armed in `ctx.state()`
  (seen at the harness's next refresh), or to retry a step a crash left
  unfinished. Otherwise nothing is written: recovery never needs a model
  step (the child's session in the journal is what a resume reads).
- **Timed from the request**, as a Pi model step is: the time to the first
  token counts (these CLIs send large system prompts), so a short
  `afterMs` can kill the call before anything streamed. Timed from the first
  streamed bytes instead, the drill did not fire in the demo: the Gateway
  sometimes delivers a whole reply in a burst.
- **Overlapping requests share one step**, closed when the last one ends.
- **The killed step is retried.** The kernel releases it (`pending`); the
  recovered turn's first model request re-enters its key, so the log shows
  attempt 2, the regenerated response, and nothing is left open.
- **A drive that stops** mid-step rejects the step with its reason (the
  kernel releases it); a write that fails halts the drive, as for a tool
  call.

### Kernel: `ctx.halt(reason)`

`DriveContext.halt` (harness-world) stops a drive from outside an effect,
for an in-band write that failed inside a harness callback the harness's
runtime swallows (a session store's `append`, a background journal flush).
It aborts `ctx.signal` with `reason` (a `Yield('preempted')` preempts), later
effects throw it without writing, and the driver treats it as the drive's
outcome whatever the harness throws on its way out: a `Restart` rebuilds the
drive from the log, a `Yield` yields, a `Fatal` fails the run, anything else
ends the delivery with that error (the queue redelivers). The journal and
the bridge call it; a port calls it for its own callbacks.

## Payload limits

From the Vercel Workflow limits (vercel.com/docs/workflows/pricing): any
step input or output up to **50 MB**, a stream chunk up to **10 MB**, up to
**10,000 steps** and **25,000 events** per run, 200 event writes per run
per second, and runs past 2,000 events or 1 GB replay slower. Writes from a
tab go through a Function (4.5 MB request body). world-local and
world-postgres impose no smaller limit we found.

Hence: records over 32 KiB go to the blob stream (the log is folded whole
on every cold drive and polled by viewers), blobs are written in pieces well
under a stream chunk, and batches are group-committed. A simple turn with
one tool is a placement, a few journal batches and the tool's own step.

## Adding a process harness to the demo

1. A file in `apps/demo/lib/harnesses/` that default-exports a
   `DemoHarness`: `value` (the harness name), `label`, `title`, `modes`
   (`['server']` until the Sandbox lane runs it), optional `durabilities`,
   `enabled()` and `disabledReason()` (a missing key or binary, read per
   request), optional `crashes` (the "Crash during" targets that fire for
   it: `['tool', 'any']` for a harness whose model traffic bypasses
   `gatewayProxy` and `ModelSteps`), and `load()`, which imports the port and
   returns its kernel `Harness`.
2. One line in `apps/demo/lib/harnesses/index.ts`'s list.

The picker, the session list and the Server-mode transcript pick it up
(`GET /api/sessions?include=process` carries the options to the page). The
transcript renders the normalized transcript, the tool effects and the
recovery note, and streams `text` live events into a row while a turn is
open. Add the port's SDK to `serverExternalPackages` in
`apps/demo/next.config.ts` if it ships native code. `HARNESS_REFERENCE=1`
registers the reference harness locally ("Reference CLI").

## What a crash costs

The model call in flight (its partial output is not in the log; the harness
regenerates it; around a drill, the `model` step it was in is retried), one
recovery turn (the note), and the tool call in flight: a `never` call comes
back interrupted, never rerun ("may have taken effect"), and a `safe` one is
reported safe to repeat. A built-in's command is killed with the drive, so
it may have partly run. Native records appended after the last batch
committed are lost, which is at most the work since the last barrier and
never a `never` call's own record.

With the built-ins on, also the working directory when the machine goes:
it is a disk, not the log. The model is told when a drive finds another
copy (`workspaceNote`).

## Not done yet

- A record-only effect is two events. A World with `events.createBatch`
  could write the pair in one request.
- "Crash during: model" for a harness whose model traffic does not go
  through `gatewayProxy`: no step to fire in, so the demo offers only tool
  crashes for it.
- Concurrent tool calls in one drive: the bridge serializes; a harness's
  own parallel calls (Claude Code's reads) overlap as effects.
- A built-in's working directory in the log, or carried between machines.
- `flock` for two processes sharing one session root (the pidfile check and
  the kernel's single writer cover the cases we have).
- The Sandbox lane: running a process harness in the VM daemon and a
  Function-lane takeover that boots a VM.

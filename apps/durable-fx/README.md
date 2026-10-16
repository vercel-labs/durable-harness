# durable-fx example

The smallest deployable durable fx agent: [`libfx`](https://github.com/vercel-labs/fx)'s
own `createFxAgent`, nothing else. libfx keeps each session in Vercel's World
(the store and queue behind Vercel Workflow), runs every turn in a
queue-delivered Function, and continues a turn after a crash, a timeout or a
redeploy from its last record. No adapter, no Workflow package to install:
libfx carries the Worlds it needs.

```sh
npm install libfx durable-harness just-bash
```

libfx is durable from 0.0.14 (in this repo the pin is the pnpm catalog
entry). `durable-harness`
gives the agent its workspace tools (`durable-harness/fx`, on `just-bash`) and
`next.config.ts` its `withDurableHarness`, which writes the route libfx's
queue delivers to ([below](#nextconfigts)); an app without Next.js mounts that
route itself ([Without Next.js](#without-nextjs)).

## The whole thing

[`lib/agent.ts`](lib/agent.ts), one agent per server:

```ts
import { virtual } from 'durable-harness';
import { fxTools } from 'durable-harness/fx';
import { createFxAgent } from 'libfx';

export const agent = createFxAgent({
  model: 'anthropic/claude-opus-5.5-fast',
  instructions: 'You are a helpful agent. Keep answers short. Your files are in /workspace.',
  tools: [...fxTools(virtual()), ...tools], // lib/tools.ts
});
```

[`app/api/chat/route.ts`](app/api/chat/route.ts), the prompt and the stream:

```ts
export async function POST(request: Request) {
  const sessionId = new URL(request.url).searchParams.get('sessionId') ?? undefined;
  const turn = agent.session(sessionId).prompt(await request.text());
  return new Response(turn.readable); // NDJSON; the first line names the session
}

export async function GET(request: Request) {
  const params = new URL(request.url).searchParams;
  return new Response(agent.session(params.get('sessionId')!).stream(Number(params.get('cursor') ?? 0)));
}
```

### next.config.ts

[`next.config.ts`](next.config.ts):

```ts
import { withDurableHarness } from 'durable-harness/next';

export default withDurableHarness({});
```

On `next build` and `next dev` it finds the module that makes the agent (it
imports `libfx` and calls `createFxAgent()`) and writes the route libfx's queue
delivers to, `app/.well-known/workflow/v1/flow/route.js`: it imports
`lib/agent.ts` and mounts the agent's `wakeHandler()`. Next to it,
`config.json` gives that Function `maxDuration: 'max'` and a queue trigger on
libfx's topic, `__libfx_wkf_workflow_session` (Vercel's Next.js builder reads
it: no `vercel.json` entry), and a `.gitignore` keeps both out of git. It also
keeps `libfx`, a native addon, out of the server bundle. (In an app that also
uses the Workflow SDK, which owns that path, the route is
`app/.well-known/durable-harness/v1/fx/route.js` instead, with one
`vercel.json` entry for its trigger: `npx durable-harness vercel-json --write`.)

The route reaches the agent through its module's exports, so export it at the
module's top level (`export const agent = createFxAgent(...)`, any name, or
`export default`); the build warns when it is not. One fx agent per app: libfx
queues every fx agent's wakes on one topic and a wake does not say whose
session it is, so with several the build warns and the route mounts the first
module that exports its agent (withDurableHarness's `agents` option picks
another). An app that keeps a wake route of its own, subscribed to libfx's
topic in `vercel.json` ([below](#without-nextjs)), keeps it: withDurableHarness
then adds no trigger.

`prompt()` stores the prompt and queues it; the turn runs in that route,
whichever instance the queue picks, while `POST` streams it. If that response
ends, the turn keeps going and the client reconnects with `GET` and the last
`cursor` it saw. On Vercel libfx authenticates to AI Gateway with the
deployment's OIDC token.

### Without Next.js

Mount the wake handler yourself, in the function the queue delivers to (here
`api/libfx.ts`, a Vercel Function):

```ts
import { agent } from '../lib/agent';

export const POST = agent.wakeHandler();
```

and subscribe that function to libfx's topic in `vercel.json`:

```json
{
  "functions": {
    "api/libfx.ts": {
      "maxDuration": "max",
      "experimentalTriggers": [{ "type": "queue/v2beta", "topic": "__libfx_wkf_workflow_session" }]
    }
  }
}
```

A Next.js app can do the same with a route of its own (`app/api/libfx/route.ts`
and that entry under its path) instead of `withDurableHarness`.

### Tools

`fxTools(virtual())` gives the model libfx's kernel tools, `read_file`,
`glob_files`, `grep_files`, `edit_file`, `write_file` and `shell`, with the
names and schemas fx's own CLI uses, on a virtual workspace at `/workspace`:
just-bash on a virtual filesystem, never this server's own files, processes
or environment. Each call's file changes are kept with the session, keyed by
the call's `executionId`, so a call libfx runs again after a crash never
applies them twice. Leave the line out and the agent has only the app's
tools: libfx adds none of its own.

[`lib/tools.ts`](lib/tools.ts) has one tool of each kind libfx tells apart:

- `wait` is `idempotent: true`: a call a crash cut off runs again, with the
  same `executionId`.
- `charge` has effects (the default): libfx stores the call before it runs and
  never runs it again by itself; after a crash the model hears that it may
  have partly run and asks. A real tool passes `executionId` to its API as the
  idempotency key.

## What happens when a Function dies

libfx on `vercel()` holds a session with a lease that the running worker
renews (a `lease` entry in the session's log each time). When the Function
dies, the next delivery takes the session over once the lease has run out,
and the turn finishes: a payment never runs twice, a model request cut off
is made again, and a `shell` call cut off runs again once, with its file
change landing once. `maxDuration` does not bound recovery; it only decides how
often a long turn yields (libfx stops before the deadline and the same
message continues it).

## Run it

From the repo root: `pnpm install`. The AI Gateway credential is
`AI_GATEWAY_API_KEY`.

**Without a server.** Off Vercel, libfx keeps sessions in local files
(`$FX_SESSIONS_DIR`, or `$TMPDIR/libfx/sessions`), so the CLI runs the same
agent in one process:

```bash
cd apps/durable-fx
pnpm cli 'I explicitly ask you to charge me $42 with the memo "order 1001", then tell me what happened.'
# kill -9 it while [charge] runs, then:
pnpm cli <sessionId>             # continues the turn: no second charge
pnpm cli <sessionId> "and now?"  # a follow-up in the same session
```

A `kill -9` in the middle of `charge` leaves one ledger line, and the resumed
run tells the user the charge may have gone through and does not retry it; a
`kill -9` in the middle of `wait` reruns `wait` once and finishes.

**As a web app** (`pnpm dev`, or deploy this directory as its own Vercel
project): a chat page on `/api/chat`. Reload mid-turn and the page follows the
same turn from the last cursor it saw.

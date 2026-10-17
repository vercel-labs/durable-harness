# harness-world-cli-check

A harness session is a Workflow run, so the stock Workflow CLI
(`@workflow/cli`, `workflow inspect …`) should show it like any other run:
listed, named, its steps readable, its payloads decoded, its streams
printable. This package checks that for every harness in the repo, on every
World it can reach, with scripted models and no credentials. CI runs it on
world-local and world-postgres (`.github/workflows/ci.yml`).

```bash
pnpm cli-check                                   # world-local, every blocking harness
pnpm cli-check reference                         # one harness (a test file name filter)
CLI_CHECK_WORLDS=local,postgres \
  WORKFLOW_POSTGRES_URL=postgres://world:world@localhost:5433/world pnpm cli-check
pnpm --filter harness-world-cli-check test       # the checks' own unit tests
```

world-postgres needs its tables first (`docker compose up -d` at the repo
root starts a database on port 5433):

```bash
WORKFLOW_POSTGRES_URL=postgres://world:world@localhost:5433/world \
  node packages/cli-check/node_modules/@workflow/world-postgres/bin/setup.js
```

The report (a harness x World matrix, then per session the run, its steps,
its streams and every finding) is written to `$CLI_CHECK_OUT/report.md`
(default `$TMPDIR/harness-cli-check`), next to the world-local data
directories, which the CLI can be pointed at afterwards. CI puts the report in
the job summary and uploads the directory when a job fails.

## What it does

Each harness gets one or two sessions (`scenarios/`):

- **basic**: a prompt that makes one tool call and gets a text answer, then a
  text-only prompt, each settled (nothing pending, the log quiet);
- **recovered** (Pi, fx, the AI SDK, the reference process harness): the
  kernel's chaos kills the drive inside a `replay: 'never'` tool, the World
  instance closes (the process is gone), a second World instance on the same
  storage recovers the session, and one more prompt goes in.

Each session is inspected twice, open and after `sessions.close()`, with the
pinned CLI in its own process (`workflow inspect runs -n`, `run`, `steps -d`,
`step -r` for one step of each name, `events -d`, `hooks`, `streams`, and
`stream` for every stream), and judged against ten invariants
(`src/invariants.ts`):

| # | Invariant | What it checks |
| --- | --- | --- |
| 1 | listed | `inspect runs -n workflow//durable-harness@<version>//<name>` lists the run |
| 2 | status | open: `running`, no `completedAt`; closed: `completed` with `completedAt`; `input` decodes to `{ harness }` |
| 3 | steps | every step is `step//harness-world//<effect>` with an effect the harness writes; the model and tool calls in order; nothing `pending` or `running`; `failed` only for the interrupted tool of a recovered session |
| 4 | payloads | step input is the kernel's envelope (`{ key, replay, input, epoch }`); inputs, outputs, errors and every `hook_received` payload decode to plain values |
| 5 | events | event ids increase; per step `step_created`, `step_started`, one terminal event; one `hook_received` per submission; a closed session ends with `run_completed` |
| 6 | hooks | open: exactly the inbox hook; closed: none |
| 7 | streams listed | the CLI lists exactly the World's streams, and every stream the kernel or a harness wrote that holds data |
| 8 | streams read | every stream decodes; a settled step's stream ends, and every stream of a closed session ends |
| 9 | no CLI errors | every call exits 0 with no error line or stack frame on stderr |
| 10 | agree | the CLI's steps and statuses equal `world.steps.list` and the kernel's `effectOrder`; its event count equals `world.events.list` |

A gap the check knows about is an entry in `src/known.ts` (with whose fix it
is), run as a strict expected failure (`it.fails`): when it is fixed its test
fails until the entry is deleted. The CLI's own quirks are pinned the same way
in `test/cli-quirks.test.ts`.

- `scenarios/`: one driver per harness.
- `src/invariants.ts`: the CLI calls (`collect`) and the checks (`evaluate`).
- `test/vercel.test.ts`: world-vercel through a deployment of `apps/demo`
  (needs `CLI_CHECK_VERCEL_URL`, `CLI_CHECK_VERCEL_PROJECT`,
  `CLI_CHECK_VERCEL_TEAM` and `VERCEL_TOKEN`; skipped otherwise).

## Agent Runs coverage

`test/agent-runs.test.ts` runs one tool turn of every server harness (Pi, Pi
with pi-durable, fx, the AI SDK, Claude Code, Codex, OpenCode) with scripted
models on world-local, through durable-harness's `agentRuns()`
(`durable-harness/instrumentation/otel`, on `@vercel/agent-runs`) with an
in-memory span processor. It checks each turn's spans as Vercel Agent Runs
reads them: the `invoke_agent` root in the `eve.agent` scope at schema 4, an
`agent.step` per model call with its `chat` span and the tool calls it asked
for under it, all in the turn's trace, and a tool span carrying the id of the
call the model's reply names (Agent Runs joins the two by it)
(`src/agent-runs.ts`). That a session that is not public records only
metadata is durable-harness's own `test/agent-runs.test.ts`.

`test/agent-runs-live.test.ts` does the same with the real CLIs and AI Gateway
models, and also checks model ids, the prompt in the model's input and token
usage. It needs `AGENT_RUNS_LIVE=1` and a Gateway credential
(`AI_GATEWAY_API_KEY` or `VERCEL_OIDC_TOKEN`; `AGENT_RUNS_ENV_FILE` loads them
from a dotenv file), and is skipped otherwise:

```bash
AGENT_RUNS_LIVE=1 AGENT_RUNS_ENV_FILE=.env.local \
  pnpm --filter harness-world-cli-check exec vitest run test/agent-runs-live.test.ts
```

## Pointing the CLI at each World

```bash
# world-local: the data directory (otherwise it looks for .workflow-data from the cwd up)
WORKFLOW_LOCAL_DATA_DIR=<dataDir> workflow inspect runs --json

# world-postgres: -b resolves the package from the current directory's node_modules,
# so run it from a package that depends on @workflow/world-postgres (apps/demo, packages/cli-check)
WORKFLOW_POSTGRES_URL=postgres://… workflow inspect runs -b @workflow/world-postgres --json

# world-vercel
WORKFLOW_VERCEL_AUTH_TOKEN=… workflow inspect steps -r <runId> -b vercel \
  --project <project> --team <team> -e production --json
```

`inspect stream`, `inspect step` and `inspect event` need `-r <runId>`, and
`WORKFLOW_NO_UPDATE_CHECK=1` skips the CLI's npm version check.

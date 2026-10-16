# harness-world-fx (private)

fx on the harness-world kernel: libfx's engine (`createFxEngine`, one
conversation in one process) driven by harness-world, with its model
responses as a tape and its tool calls as kernel effects in the session's
log. It is the fx lane where a session has to move between places: the
browser tab (libfx's Wasm build), Hybrid and the sandbox VM, the Cloudflare
World, and the CLI check. It is private (not published).

**For an app, use libfx directly.** Since vercel-labs/fx#1180, libfx is
durable by itself: `createFxAgent` keeps each session in a World and
continues it after a crash. No adapter:

```ts
import { createFxAgent } from 'libfx'; // libfx 0.0.14 or later

export const agent = createFxAgent({ model: 'anthropic/claude-opus-5.5-fast', tools });

// app/api/chat/route.ts
export const POST = async (req: Request) =>
  new Response(agent.session(new URL(req.url).searchParams.get('sessionId') ?? undefined).prompt(await req.text()).readable);

// next.config.ts: withDurableHarness writes the wake route and its queue trigger
export default withDurableHarness({}); // durable-harness/next
```

Without Next.js, the wake route is the app's own (`export const POST =
agent.wakeHandler()`), subscribed in `vercel.json` to
`__libfx_wkf_workflow_session`.

The deployable version, with a reconnect route, a page and a CLI, is
[apps/durable-fx](../../apps/durable-fx). In the demo it is the
**fx (libfx durable)** harness (`apps/demo/lib/fx-durable.ts`), which keeps
libfx's sessions in the app's World: libfx's `vercel()` on Vercel, and on
world-local the app's own world-local through `world()` from
`libfx/durable-world` (`apps/demo/lib/fx-durable-world.ts`).

For coding tools, give that agent `fxTools(virtual())` from
`durable-harness/fx`: libfx's kernel tools (`read_file`, `glob_files`,
`grep_files`, `edit_file`, `write_file`, `shell`) on a virtual workspace, or
`fxTools(sandbox())` in the session's own Vercel Sandbox (armed with the
epoch of the libfx lease the call's worker holds). The kernel-driven harness
here takes the same tools as `HostTool`s, `fxHostTools(workspace)` from
`harness-world-hands/fx`, their file changes committed with each call's
outcome in the session's log; on a sandbox,
`fxHostTools(sandboxRuntime(kernelSandbox(acquire, load)))` (from
`harness-world-hands/runtime`), whose tools carry a pre-intent phase
(`beforeIntent`, the sandbox's attach, run before the call's intent is
written) and a drive-end hook (`afterDrive`), which `fxHarness` runs.

## In this package

- `src/index.ts`: `fxHarness({ createFxEngine, apiKey, model, tools })`, the
  kernel `Harness`. libfx's `createFxAgent` passed as the engine is refused
  with a TypeError that says what to pass.
- `src/durable.ts`: `createDurableFxAgent`, a libfx-shaped handle on a kernel
  session, kept for the kernel's tests. Apps use libfx's `createFxAgent`.
- `test/sandbox.test.ts`: fx's tools on a sandbox, for libfx's own agent and for this harness, on harness-sandbox's
  stand-in through the real session factory; on a live Vercel Sandbox with
  `DH_LIVE_SANDBOX=1` (`DH_LIVE_SANDBOX_DIR`: a directory linked to a Vercel
  project, the Vercel CLI logged in).

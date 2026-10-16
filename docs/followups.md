# Follow-ups

Open work that a test or a note in the code points at.

## libfx 0.0.14

The workspace moved from the libfx 0.0.13-dev.1432 build to 0.0.14, the
first stable with durability. Four tests are skipped until they are
reworked for it:

- **The kernel lane's steering** (`packages/harness-fx/src/fx.test.ts`,
  "does not place a steer on a request that is already on the tape",
  "keeps a retried model request exactly as its first attempt recorded it"
  and "delivers a steer taken while the request it followed was
  interrupted"). harness-world-fx records each steer on the tape and hands
  it to the kernel only where the next model request is live. On 0.0.14 the
  kernel places a steer differently from the tape, so a replay diverges; a
  steer can join a retried request, depending on timing (it does on CI under
  vitest 4); and a steer re-offered while a crashed turn replays its
  interrupted request is never sent. This is the private kernel harness (the demo's kernel fx lane),
  not `durable-harness/fx` or libfx's own `createFxAgent`. It needs
  harness-world-fx's steer step matched to 0.0.14's `FxTurn.steer(input, id)`.
- **P0.12 across two processes** (`packages/harness-fx/test/sandbox.test.ts`).
  The test runs two servers in one process. libfx 0.0.14 runs one
  configuration per agent name on a store in a process, and routes a World's
  deliveries over its agents by name, so the second worker no longer gets the
  session. The fence it checks (FxSandbox's lease epochs) is unchanged, and
  "files last across VMs" covers the next worker's higher token. It needs the
  second worker in a process of its own, with the Sandbox stand-in shared
  between them.

---
"durable-harness": patch
---

libfx's own durable agents show up in Vercel Agent Runs: `createFxAgent({ hooks: agentRunsHooks() })` with `agentRunsHooks` from `durable-harness/fx` records each libfx turn, its steps with their model calls, and its tool calls and the approvals they wait on, from libfx's semantic hooks (libfx 0.0.14 and later). A turn libfx yields, pauses or hands to another worker (at its deadline, or when another worker's write fences it out) stays one turn, continued by the execution that resumes it in any process, and a call the handoff cut off runs again as the same call. With `agentRuns()` registered first, libfx passes content only where the audience records it.

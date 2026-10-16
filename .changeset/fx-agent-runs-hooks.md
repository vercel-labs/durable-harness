---
"durable-harness": patch
---

libfx's own durable agents show up in Vercel Agent Runs: `createFxAgent({ hooks: agentRunsHooks() })` with `agentRunsHooks` from `durable-harness/fx` records each libfx turn, its steps with their model calls, and its tool calls and the approvals they wait on, from libfx's semantic hooks (libfx 0.0.14 and later). A turn libfx yields, pauses or hands to another worker stays one turn, continued by the execution that resumes it in any process.

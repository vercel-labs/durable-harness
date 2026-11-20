---
"durable-harness": patch
---

Agent Runs tracing (`agentRuns()` from `/instrumentation/otel`) uses `@vercel/agent-runs@1.1.0` and writes its spans under the library's default `vercel.ai.tracing` instrumentation scope, the one Vercel Agent Runs reads, instead of `eve.agent`. Upgrade the optional peer to 1.1.0 (`pnpm add @vercel/agent-runs@1.1.0`): with 0.0.1 installed, spans use that version's default `agent.tracing` scope and no longer appear in Agent Runs.

---
"durable-harness": patch
---

Agent Runs tracing (`agentRuns()` from `/instrumentation/otel`) records sessions with `@vercel/agent-runs`, the library eve uses: a turn per prompt, a step per model call with the tool calls it asks for under it, in the schema Agent Runs reads. A turn that goes on in a later invocation (out of time, a crash, a wake, a lane handoff) stays one turn, its checkpoint kept with the session. A session's `traceAudience` decides what its spans record (`public`: inputs and outputs; anything else: metadata only), and `agentRuns({ capture })` changes that per audience; `exportPolicy` and `filteringProcessor` are removed. Claude Code, Codex and OpenCode record each model request through the Gateway with its prompt, reply and usage. Requires Node.js 24.

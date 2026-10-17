---
"durable-harness": patch
---

Agent Runs tracing (`agentRuns()` from `/instrumentation/otel`) records sessions with `@vercel/agent-runs@0.0.1`, the library eve uses: a turn per prompt, a step per model call with the tool calls it asks for under it, in the schema Agent Runs reads. A turn that goes on in a later invocation (out of time, a crash, a wake, a lane handoff) stays one turn, its checkpoint kept with the session. A session's `traceAudience` decides what its spans record (`public`: inputs and outputs; anything else: metadata only), and `agentRuns({ capture })` changes that per audience; `exportPolicy` and `filteringProcessor` are removed. Claude Code, Codex and OpenCode record each model request through the Gateway with its prompt, reply and usage.

Recovery applies the current capture policy to restored operations, keeping newly private content out of checkpoints and exports. Recorded tool definitions exclude provider credentials, untraced proxy requests stream without buffering, and tracing failures cannot change agent results. Checkpoint waits are bounded without discarding final updates after a soft budget expires or a write slows down; queued follow-ups retain their own turn identity.

Agent Runs is an optional peer with inlined declaration types. Tracing requires Node.js 24 and optional tracing peers; other entrypoints retain Node.js 22 support.

---
"durable-harness": patch
---

libfx's own durable agents show up in Vercel Agent Runs: `createFxAgent({ hooks: agentRunsHooks() })` with `agentRunsHooks` from `durable-harness/fx` records each libfx turn, its steps with their model calls, and its tool calls and the approvals they wait on, from libfx's semantic hooks (libfx 0.0.14 and later). A turn libfx yields, pauses or hands to another worker stays one turn, continued by the execution that resumes it in any process.

Handed-off and fenced turns stay open for recovery; actual user interruption and cancellation finish the turn. Interrupted tool and approval identities are checkpointed so final cleanup also settles recovered calls the engine does not rerun. Execution IDs isolate overlapping deliveries, and ended session bookkeeping is released. Capture follows policy, hook failures use the tracing error handler, and provider/reasoning metadata and turn ordering are preserved. Unawaited hooks and checkpoint flushing remain best-effort.

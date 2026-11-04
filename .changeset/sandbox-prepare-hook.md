---
"durable-harness": patch
---

Add `prepare` to `claudeCode()` and `codex()`: the app's own setup of a session's Drive, run once per Drive before the CLI starts, with credentials allowed in its commands' env.

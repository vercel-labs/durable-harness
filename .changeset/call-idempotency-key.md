---
"durable-harness": patch
---

Tools get `io.idempotencyKey()`: a key that is the same on every run of a call and resolves once the call is in the log, so a tool that creates something can be `replay: 'safe'`.

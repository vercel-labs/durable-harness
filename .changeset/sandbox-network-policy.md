---
"durable-harness": patch
---

`sandbox({ network })` takes a full Vercel Sandbox `NetworkPolicy` (per-domain rules, including header transforms for credential brokering, and subnets) or a per-session callback, for every harness that runs in a session's Sandbox (Claude Code, Codex, OpenCode, Pi, fx, and pi-durable's `sandboxEnv()`). The harness applies it again on every attach, so set the VM's policy here rather than with `sandbox.update`; header values are kept out of the session's log, its spans, log lines and errors.

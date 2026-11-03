---
"durable-harness": patch
---

`sandbox({ network })` takes a full Vercel Sandbox `NetworkPolicy` (per-domain rules, including header transforms for credential brokering, and subnets) or a per-session callback, and the harness applies it again on every attach, so set the VM's policy here rather than with `sandbox.update`; header values are kept out of the session's logs and errors.

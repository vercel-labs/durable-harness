---
"durable-harness": patch
---

A session's run id is minted by the World's `createRunId()` when it has one, as the Workflow SDK's `start()` does: on world-vercel the id carries the region the Function runs in, so the session's log is kept there instead of in the default region.

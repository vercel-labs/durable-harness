---
"durable-harness": patch
---

`sessions.close` stops the session's sandbox VM by default (`sandbox({ onClose })` or `close(id, { sandbox })`: `stop`, `delete` or `keep`), and the README documents `deleteSessionSandbox` and `sandboxCleanup`.

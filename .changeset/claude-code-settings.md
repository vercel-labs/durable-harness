---
"durable-harness": patch
---

`claudeCode()` accepts `settings`, Claude Code settings passed to the Agent SDK's `query()` as its flag settings. `settings.sandbox` and `settings.env` are refused with a `TypeError`, since the harness sets Claude Code's sandbox and the CLI's environment itself.

---
"durable-harness": patch
---

The model proxy passes on the four `anthropic-beta` values Claude Code 2.1.296 adds with a model that takes an effort and per-turn settings (such as claude-opus-5-5): without them, a provider that checks the betas (the Anthropic API as `gatewayUrl`) answers 400 to every turn.

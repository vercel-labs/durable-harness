---
"durable-harness": patch
---

Document what each harness does with a `prompt`, `followUp` or `steer` sent while a turn runs, and stop `durable-harness/pi` from handing a mid-turn prompt to Pi twice when its inbox is read just before the model call that places it commits.

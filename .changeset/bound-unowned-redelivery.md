---
"durable-harness": patch
---

Stop redelivering a queue message no agent in the process owns after a bounded number of deliveries, then acknowledge it with an error that names its run.

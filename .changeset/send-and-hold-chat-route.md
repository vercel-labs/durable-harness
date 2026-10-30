---
"durable-harness": patch
---

The chat route starts a turn in the request itself as a held queue message (Vercel Queues' send-and-hold) wherever the World supports it: no 60 s deadline and no delayed companion, with the queue redelivering the turn if the invocation dies

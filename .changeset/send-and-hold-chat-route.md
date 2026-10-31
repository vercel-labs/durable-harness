---
"durable-harness": patch
---

The chat route starts a turn in the request itself as a held queue message (Vercel Queues' send-and-hold) wherever the World supports it: no 60 s deadline and no delayed companion, with the queue redelivering the turn to the flow route if the invocation dies. The hold is claimed for the flow route's consumer group, which the build records (`default` for the generated Next.js route, the name Vercel derives from the `functions` key in `vercel.json` / `vercel.ts`); where it cannot be known, nothing is held

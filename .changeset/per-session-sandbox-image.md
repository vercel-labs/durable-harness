---
"durable-harness": patch
---

`sandbox({ image })` also takes a function of the session, called each time a new VM is created for it (first boot and every restart), so each session can boot from its own image or snapshot.

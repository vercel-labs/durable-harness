# The chat route's 60 s handover, and send-and-hold

Why `chatRoute` drives a turn for at most 60 seconds before the queue takes
it over, what that costs, and the queue feature that removes the limit:
**send-and-hold**, a message born leased to its sender. The public docs say
only that this is a current Vercel Queues limitation with a fix on the way,
and that `chatRoute({ drive: 'queue' })` avoids it meanwhile.

## How a turn is driven today

A drive's liveness is the lock on a queue message, which the queue library
renews while the handler runs. A crash means the lock lapses and the queue
redelivers the same message. Nothing in this repo writes a heartbeat. Every
drive runs under one **anchor**, recorded on each intent it writes (see
`packages/harness-world/src/anchor.ts`):

- `msg`: a queue delivery (the flow route). The queue's lock on that message
  holds it.
- `deadline`: a drive that holds no message, such as `chatRoute`'s default
  `drive: 'route'` (`sessions.drive()`). Its own deadline holds it: the drive
  writes nothing after the deadline, and a **companion** queue message,
  delayed past the deadline, stands behind it.

So `chatRoute` with `drive: 'route'` (the default) works like this:

1. Before driving, the request sends the companion, delayed
   `ceil(deadline - now) + 5` seconds.
2. It drives the turn in its own invocation, so the first words stream at
   once, under a `deadline` anchor (`directDriveMs`, default 60 s, capped by
   the invocation's own limit).
3. If the turn ends first, the companion arrives later, finds the turn
   answered, and drops.
4. If the drive still has work near its deadline, it hands over through a
   continuation. The flow route then drives on as a queue delivery, with the
   plan's maximum duration.
5. If the invocation dies silently, the companion finds the anchor past its
   deadline and recovers the turn: about 65-70 s after the submit.

The fixed deadline is how a drive that holds no message bounds a silent
death. The only alternative without a held message is a timer of our own
(heartbeats), and this design removed those.

## What the deadline costs

No effect starts in the deadline's last 15 s (`deadlineMarginMs`). An effect
still running at the deadline is cut, and the continuation decides what to do
with it from its replay policy:

- a `safe` one (a model call, a read) is run again;
- a `never` one (a command, a payment, anything whose second run could do
  harm) is marked interrupted and never run again. The model is told it
  stopped.

So a long command early in a turn is cut: an `npm install` or a build that
starts 30 s in, or a Sandbox's first attach that builds the image. The
workaround is `chatRoute({ drive: 'queue' })`. The whole turn then runs in
queue deliveries, whose lock is renewed for as long as the handler runs, at
the cost of one queue hop before the first words.

pi-durable's request path is different. Its storage renews its own deadline
anchor and companion on every commit (`recoverAfterMs`, default 120 s), so
long turns are fine, but one command running past that with nothing committed
is exposed in the same way.

## Send-and-hold

The fix is a message the sender holds from the moment it sends it, renewed
while the sender runs: the same lock a delivery has, but taken at send time.

- **Vercel Queues: send-and-hold.** Send a message born leased to its
  sender. The queue doesn't deliver it while the lease is held. The sender's
  queue client renews the lease while it runs, as it already renews a
  delivery's. When the sender dies, the lease lapses and the queue delivers
  the message, exactly as it redelivers a crashed delivery's.
- **The World: hold on send.** A World that supports it declares
  `capabilities.experimental_queueHold`. `world.queue(name, payload, {
  idempotencyKey, experimental_hold: {} })` (never with `delaySeconds`)
  resolves with `experimental_held` when the queue holds the message, or
  `experimental_notHeld: { reason }` when it doesn't. `held.consume(handler)`
  runs the queue consumer's own handler on the message in the sender, exactly
  as a pushed delivery runs (first delivery, lease renewed while it runs, ack
  on resolve, retries on a rejection). world-vercel claims the message for the
  consumer group `default`, the one the generated Next.js flow route
  subscribes with.

With both, a `wake: 'backstop'` submit (`chatRoute`'s default `drive:
'route'`, the demo's drive route) sends the turn's wake undelayed and held,
under the key the companion would have had (`harness-world`'s `hold.ts`):

- **Held.** The submit starts `held.consume(sessions.consume)` at once, under
  the caller's `waitUntil`, from the log it just wrote (run-ahead included).
  The drive runs under the held message's own `msg` anchor: no deadline
  anchor, no companion, no deadline margin. The `drive()` the drive route
  calls next waits for that delivery instead of driving. It is an ordinary
  delivery to the kernel, so one drive per session per process still holds:
  a held wake that lands while the session drives here acks, and the drive
  under way runs once more.
- **Not held** (any reason). Nothing runs here: the queue delivers the wake to
  the flow route, one hop, as `drive: 'queue'` does. A run-ahead submission
  sends one more wake once it is durable, so a delivery that came first and
  found nothing to do does not leave it waiting. An `unsupported` answer (the
  queue has send-and-hold off) sends the process back to the deadline path
  for a few minutes before it asks again, so a World that declares the
  capability ahead of its queue costs one hop, not one per turn.
- **No capability** (world-local, world-postgres, the browser), or
  `HARNESS_QUEUE_HOLD=0`: the deadline anchor and its delayed companion,
  unchanged.

What changes for a held turn:

- No fixed 60 s. The request drives as long as its invocation lives (up to
  its `maxDuration`), the World renewing the lease, so commands get their
  full time. At the invocation's own limit the drive hands over through a
  continuation, as any queue delivery does.
- A silent death is recovered when the lease lapses, which takes seconds,
  where with a deadline it's the deadline plus 5 s.
- A harness that cannot run in the chat route's bundle hands the turn to the
  queue at once (a continuation), as a deadline drive with a companion does.
- A chaos submission (`sessions.chaos(..., { wake: 'backstop' })`) keeps its
  companion: it arms a crash for a later drive and must not wake one now.

pi-durable's storage still renews its own deadline anchor and companion on
every commit.

Status: the World API is experimental (vercel/workflow#5159). It takes
`@workflow/world-vercel` with it and Vercel Queues with send-and-hold
enabled; before that the World declares no capability and the deadline path
runs as before. Until then the docs tell readers to pass `drive: 'queue'` for
agents that run long commands.

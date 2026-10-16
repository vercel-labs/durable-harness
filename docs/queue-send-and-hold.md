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
- **The World: hold on send.** `world.queue(name, payload, { hold: true })`
  returns `{ messageId, lease }`. The lease carries `signal` (aborted if the
  lease is lost), `release(delaySeconds)` and `deadline`: the same handle a
  delivery's metadata would carry (`meta.lease`).

With both, `chatRoute` sends the turn's message held and drives under a `msg`
anchor on that message instead of a deadline:

- No fixed 60 s. The request drives as long as its invocation lives (up to
  its `maxDuration`), renewing the lease, so commands get their full time.
- At the invocation's own limit it releases the message (`release(0)`), and
  the flow route takes the turn over from the log, as a continuation does
  today.
- A silent death is recovered when the lease lapses, which takes seconds,
  where today it's the deadline plus 5 s.
- `directDriveMs`, the deadline anchor's companion and the deadline margin
  go away for the chat route. pi-durable's storage holds its own message the
  same way, in place of renewing a deadline on each commit.

Status: in progress in Vercel Queues and the Workflow SDK's World. Until it
ships, the docs tell readers to pass `drive: 'queue'` for agents that run
long commands.

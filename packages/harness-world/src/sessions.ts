import { HOOK_RESUME_DEDUP_VERSION, type CreateEventParams, type World } from '@workflow/world';
import { monotonicFactory } from 'ulid';
import { encode } from './codec.ts';
import type { Anchor } from './anchor.ts';
import {
  anchorsOf,
  type DeliverMeta,
  directDeadline,
  type DriverOptions,
  deliver,
  QUEUE_PREFIX,
  wake,
} from './driver.ts';
import { tabLost, type TabLostMessage } from './tab-lost.ts';
import { type TetherOptions, tetherDelivery } from './tether.ts';
import type { RunEnvelope, SubmissionPayload } from './fold.ts';
import { fireOwnerChaos, type OwnerMessage, ownerCheck } from './owners.ts';
import { harnessOf, workflowNameFor } from './names.ts';
import { CONSUMER_GROUP, type HoldQueueResult, queueHoldDisabledByEnv, wakeHeld, worldHolds } from './hold.ts';
import { EXECUTION_CONTEXT } from './queue.ts';
import { SessionLog } from './session-log.ts';
import { closeRunStreams, JsonLinesWriter, kernelStreamNames, liveStreamName, type TailEntry, type TailOptions, tailJsonLines, tailJsonLinesAt } from './streams.ts';
import { publishHint } from './hints.ts';
import { mark, markTime, processInfo } from './marks.ts';
import { marksEnabled, RUN_AHEAD_DEMOTE_MS, runAheadFor } from './driver.ts';
import { PipelinedLog } from './pipelined-log.ts';
import type { ChaosSpec } from './chaos.ts';
import {
  type DeepReadonly,
  type Doc,
  DocConflict,
  docLiveEvent,
  docsHookToken,
  type DocsPayload,
  type DocSnapshot,
  encodeEdit,
  materializeDoc,
  prepareUpdate,
  storedBytes,
  readDoc,
} from './docs.ts';
import type { Harness, SessionState, Submission } from './types.ts';

const ulid = monotonicFactory();

export interface DurableSessionsOptions
  extends Omit<DriverOptions, 'harness' | 'world'> {
  world: World;
  /** One harness, or several (pick one per session with `start({ harness })`). */
  harness: Harness | Harness[];
  /**
   * How this process reaches a session's sandbox daemon, to serve
   * sandbox-lane tethers (tether.ts): a delivery that holds its queue
   * message while the daemon drives, and takes the lane over if the daemon
   * goes away. Only for the process whose World queue is durable (the server).
   */
  tether?: TetherOptions;
  /**
   * Messages on the same queue that belong to someone else (another runtime
   * sharing the flow route, such as pi-world's wakes): return a promise for
   * the ones you take, `undefined` for the rest (or a promise of `ROUTE_PASS`,
   * when telling takes a read). Asked before anything else.
   */
  route?: QueueRoute;
  /**
   * The queue consumer group that consumes the kernel's topic
   * (`__harness_wkf_workflow_*`) in this deployment. With it, on a World that
   * declares `capabilities.experimental_queueHold`, a `wake: 'backstop'`
   * submission's wake is sent held (send-and-hold, hold.ts): undelayed,
   * claimed for this process under this group, and run here as a delivery
   * of that message, under its own `msg` anchor, instead of a drive under a
   * deadline with a delayed companion behind it. A wake that comes back not
   * held is the queue's to deliver (one hop, as `wake: 'queue'`).
   * Without it nothing is held: a hold claimed for the wrong group would be
   * delivered again to the group that really consumes the topic. On Vercel
   * that is `default` for a route whose trigger is in a Build Output
   * `config.json`, and the name Vercel derives from the `functions` key for
   * one configured in `vercel.json` / `vercel.ts`. `[A-Za-z0-9_-]+`.
   * `HARNESS_QUEUE_HOLD=0` in the environment turns holds off.
   */
  queueHoldConsumerGroup?: string;
}

/** See `DurableSessionsOptions.route`. Resolve `{ timeoutSeconds }` to have the queue redeliver later. */
export type QueueRoute = (message: unknown, meta: { attempt: number; queueName: string; messageId: string }) => Promise<unknown> | undefined;
/** What a `QueueRoute` resolves to hand a message back to the sessions after all. */
export const ROUTE_PASS: unique symbol = Symbol('harness-world.route-pass');

export interface StartOptions {
  /** Which harness drives the session, by name. Defaults to the first. */
  harness?: string;
  /** Harness config recorded on `run_created`. Must not contain secrets. */
  config?: unknown;
  /** Optional first prompt. */
  prompt?: string;
  /** How the first prompt wakes a driver (see `SendOptions`). */
  wake?: SendOptions['wake'];
}

export interface SendOptions {
  /**
   * Stable caller identity, scoped to this session. Retrying the same kind/text
   * returns its original submission id; a different payload throws
   * `SubmissionConflict`. Requires a session with run-ahead disabled.
   */
  identity?: string;
  /**
   * How the submission wakes a driver:
   * - `queue` (default): one queue message, delivered now (the flow route).
   * - `backstop`: the caller drives the session itself right away (the
   *   drive route, `drive()` under `waitUntil`), under a deadline anchor:
   *   it stops writing by its deadline (`DriverOptions.directDriveMs`), and
   *   the queue message, delayed past that deadline, is its backstop (its
   *   companion). If the drive died, the companion recovers the session; if
   *   it finished, the companion finds the turn answered and drops.
   *   On a World whose queue holds messages for their sender, with the
   *   consumer group known (`DurableSessionsOptions.queueHoldConsumerGroup`), the wake is sent undelayed and
   *   held instead, and this process drives the session as that message's
   *   delivery (`drive()` then waits for it): no deadline, no companion. A
   *   wake the queue does not hold is delivered by the queue, and `drive()`
   *   does nothing. A `chaos` submission keeps the companion either way: it
   *   must not wake a drive now.
   */
  wake?: 'queue' | 'backstop';
  /**
   * Hand the work after the commit (hint and mark flushes, a companion's
   * queue send) to this (Vercel's `waitUntil`, Next's `after`) and return as
   * soon as the submission is durable. Without it, `send` awaits that work,
   * concurrently.
   */
  waitUntil?: (work: Promise<unknown>) => void;
}

export class SubmissionConflict extends Error {
  constructor(readonly sessionId: string, readonly identity: string, readonly submissionId: string) {
    super('harness-world: submission identity already belongs to a different payload');
    this.name = 'SubmissionConflict';
  }
}

export interface UpdateDocOptions {
  /**
   * Apply only at this revision; otherwise throw `DocConflict` without
   * retrying. Checked before writing, and again by the fold (the edit lands
   * only on the revision it was computed from).
   */
  expectRevision?: number;
  /** Without `expectRevision`: how many times to re-read and run `mutate` again after losing a race. Default 5; then `DocConflict`. */
  retries?: number;
  /** Hand the live `doc` event's write to this (Vercel's `waitUntil`) instead of awaiting it. */
  waitUntil?: (work: Promise<unknown>) => void;
}

/** How long a submit's log is handed to a following `drive` of the same session. */
const RECENT_LOG_MS = 2_000;
/** How long a process that was told holds are `unsupported` goes back to deadline drives before it asks again. */
const HOLD_RETRY_MS = 5 * 60_000;
/** Sessions whose last submit's log this process keeps for the next submit. */
const MAX_CACHED_LOGS = 256;

export interface DurableSessions {
  /**
   * The queue consumer, for the kernel's prefix (`__harness_wkf_workflow_`):
   * register it on the World in process (`world.registerHandler(QUEUE_PREFIX,
   * handler)`), or mount it at a route the World delivers to
   * (`world.registerRoute(QUEUE_PREFIX, path)`; on Vercel, a Function
   * subscribed to `__harness_wkf_workflow_*`).
   */
  handler: (request: Request) => Promise<Response>;
  /**
   * What `handler` does with one queue message, without the request: for a
   * consumer that serves several things on one queue and has already decoded
   * the message (it decides which of them a message is for).
   */
  consume: (message: unknown, meta: { attempt: number; queueName: string; messageId: string }) => Promise<unknown>;
  /** Create a session: a run, parked on an inbox hook. */
  start(options?: StartOptions): Promise<{ sessionId: string }>;
  /** Submit a prompt, steer, or follow-up. Returns the submission id. */
  send(
    sessionId: string,
    text: string,
    kind?: Exclude<Submission['kind'], 'withdraw'>,
    options?: SendOptions
  ): Promise<string>;
  /**
   * Drive the session in this process now (what a queue delivery does),
   * holding its event channel open. For a route that serves the hot path
   * itself instead of waiting for a queue hop: call it under `waitUntil`.
   * The drive holds no queue message, so it runs under a deadline anchor
   * (anchor.ts): the one a `send(..., { wake: 'backstop' })` just declared
   * for this session, else a new one, with a companion message sent behind
   * it first where the World's queue outlives this process. After a send
   * whose wake went out held (`SendOptions.wake`), the first `drive()` of
   * that session drives nothing itself: it waits for the held message's
   * delivery in this process, or returns at once if the queue did not hold it.
   */
  drive(sessionId: string, meta?: DeliverMeta): Promise<{ timeoutSeconds: number } | undefined>;
  /**
   * Withdraw a queued submission. It is withdrawn if the withdraw commits
   * before a model step places it; otherwise it was already placed.
   */
  withdraw(
    sessionId: string,
    submissionId: string
  ): Promise<'withdrawn' | 'already_placed'>;
  /**
   * Arm a crash: the next matching effect kills its process partway
   * through (drivers must run with `chaos: true` or `HARNESS_CHAOS=1`).
   * `wake: 'backstop'` arms it without waking a driver now (only the
   * delayed companion): for a crash a drill drive of its own will fire
   * (`ChaosDriveOptions.fire: 'armed-in-drive'`), so no other drive is
   * awake to take the drill's message when it comes.
   */
  chaos(sessionId: string, spec: ChaosSpec, options?: Pick<SendOptions, 'wake' | 'waitUntil'>): Promise<string>;
  /** Close the session: `run_completed`. */
  close(sessionId: string): Promise<void>;
  /** The fold of the session's log. */
  inspect(sessionId: string): Promise<SessionState>;
  /**
   * A document (docs.ts), folded from the log (a fresh read) and migrated to
   * this definition: `initial()` at revision 0 if it was never written.
   */
  doc<T extends object>(sessionId: string, doc: Doc<T>): Promise<DocSnapshot<T>>;
  /**
   * An out-of-band edit (a route, a webhook): read the document, run
   * `mutate` on a copy, and append the change. It lands only on the revision
   * it was computed from; when something else landed first it re-reads and
   * runs `mutate` again (up to `retries`), or with `expectRevision` throws
   * `DocConflict` at once. Resolves once the edit is in the log, with the
   * document after it. No change, no write. Not atomic with anything a drive
   * does: tools use `io.docs`. Refuses a session that is not running.
   */
  updateDoc<T extends object>(sessionId: string, doc: Doc<T>, mutate: (draft: T) => void | T, options?: UpdateDocOptions): Promise<DocSnapshot<T>>;
  /**
   * Where the live stream ends right now, as a `startIndex` for `live()`:
   * tail from here to see only what happens next.
   */
  liveEnd(sessionId: string): Promise<number>;
  /** Live harness events for UIs. Pass `cursor` (from `liveEntries`) to continue where a reader left off. */
  live(sessionId: string, options?: TailOptions): AsyncGenerator<unknown>;
  /**
   * `live`, with a cursor on every event (`{ value, cursor }`). A tail
   * started from an event's cursor, in any process, continues with the
   * event after it: nothing lost, nothing twice.
   */
  liveEntries(sessionId: string, options?: TailOptions): AsyncGenerator<TailEntry>;
}

export function durableSessions(
  options: DurableSessionsOptions
): DurableSessions {
  const { world } = options;
  const harnesses = [options.harness].flat();

  const policy = anchorsOf(options);
  const holdGroup = options.queueHoldConsumerGroup;
  if (holdGroup !== undefined && !CONSUMER_GROUP.test(holdGroup)) {
    throw new TypeError(`harness-world: queueHoldConsumerGroup must match ${CONSUMER_GROUP} (got ${JSON.stringify(holdGroup)})`);
  }
  const holds = holdGroup !== undefined && worldHolds(world) && !queueHoldDisabledByEnv();
  /**
   * Until when this process sends no holds: a World can declare the
   * capability while its queue has send-and-hold off (`unsupported`), and
   * each such send costs a queue hop where a deadline drive would have
   * started at once. Asked again after a while, so turning it on reaches a
   * running process.
   */
  let holdOffUntil = 0;
  /**
   * One queue message, as the World's queue handler delivers it. `extra` is
   * what a held message's delivery in the process that sent it adds
   * (`sendHeld`): the log its submit just wrote, and that it runs in the
   * sender rather than the queue's consumer (`DeliverMeta.held`).
   */
  const consumeWith = async (message: unknown, meta: { attempt: number; queueName: string; messageId: string }, extra: Pick<DeliverMeta, 'log' | 'held'> = {}): Promise<unknown> => {
    const routed = options.route?.(message, meta);
    if (routed) {
      const result = await routed;
      if (result !== ROUTE_PASS) return result;
    }
    const { runId, input } = message as {
      runId: string;
      input?: { kind?: string; companion?: boolean; guards?: string; continues?: string };
    };
    // The retired backstop's messages still in flight: nothing to do.
    if (input?.kind === 'backstop') return;
    if (input?.kind === 'owner-check') return ownerCheck(options, runId, input as Extract<OwnerMessage, { kind: 'owner-check' }>);
    if (input?.kind === 'chaos-fire') return fireOwnerChaos(options, runId, input as Extract<OwnerMessage, { kind: 'chaos-fire' }>);
    // This delivery's anchor is its own message: the queue keeps it locked
    // while this runs, and redelivers it if this dies.
    const anchor = policy.own(meta ?? {});
    if (input?.kind === 'tab-lost') return tabLost({ ...options, openChannel: undefined }, runId, input as TabLostMessage, { anchor, attempt: meta?.attempt });
    if (input?.kind === 'tether') {
      if (options.tether) return tetherDelivery({ ...options, openChannel: undefined }, runId, { anchor, attempt: meta?.attempt }, options.tether);
      // No way to reach a daemon from here: an ordinary delivery.
    }
    // The World's queue handler holds this delivery's event channel itself
    // (world-vercel opens one per delivery), so no second, lingering claim.
    return deliver({ ...options, openChannel: undefined }, runId, {
      anchor,
      ...(meta?.attempt !== undefined ? { attempt: meta.attempt } : {}),
      ...(typeof input?.continues === 'string' ? { continues: input.continues } : {}),
      ...(input?.companion ? { companion: true } : {}),
      ...(input?.companion && typeof input.guards === 'string' ? { guards: input.guards } : {}),
      ...(extra.held ? { held: true } : {}),
      ...(extra.log ? { log: extra.log } : {}),
    });
  };
  const consume: DurableSessions['consume'] = (message, meta) => consumeWith(message, meta);
  const handler = world.createQueueHandler(QUEUE_PREFIX, consume);

  /**
   * Held drives a submit started (`sendHeld`), by session, for the `drive()`
   * that follows the submit: it waits for the held drive instead of driving.
   * Each is taken by one `drive()`; one that settled is kept for as long as
   * a submit's log is (`RECENT_LOG_MS`), so a `drive()` that comes after a
   * wake the queue did not hold still knows to leave it to the queue.
   */
  const paired = new Map<string, { done: Promise<void>; settledAt?: number }>();
  const takePaired = (sessionId: string): Promise<void> | undefined => {
    const entry = paired.get(sessionId);
    paired.delete(sessionId);
    if (!entry || (entry.settledAt !== undefined && Date.now() - entry.settledAt > RECENT_LOG_MS)) return undefined;
    return entry.done;
  };

  /**
   * A `wake: 'backstop'` submission's wake, sent held (hold.ts). Held: this
   * process runs it now, through `held.consume`, as a delivery of that message
   * (the queue's lease, renewed while it runs, is its anchor), starting from
   * the log the submit just wrote. Not held: the queue delivers it; nothing
   * runs here. `sent` resolves once the queue answered (and a held drive
   * started), `done` once that drive is over.
   */
  const sendHeld = (sessionId: string, key: string, log: SessionLog): { sent: Promise<boolean>; done: Promise<void> } => {
    let started!: (held: boolean) => void;
    const sent = new Promise<boolean>((resolve) => (started = resolve));
    const done = (async () => {
      let result: HoldQueueResult;
      try {
        result = await wakeHeld(world, sessionId, key, holdGroup!);
      } catch (err) {
        // It may have been enqueued (held, even): the plain wake under the
        // same key is the same message then, and the only one otherwise.
        options.log?.('submit: the held wake failed; waking through the queue', { runId: sessionId, error: String(err) });
        try {
          await wake(world, sessionId, key);
        } finally {
          started(false);
        }
        return;
      }
      const held = result.experimental_held;
      if (!held) {
        const reason = result.experimental_notHeld?.reason ?? 'unsupported';
        options.log?.('submit: the queue did not hold the wake; it delivers it', { runId: sessionId, reason });
        if (reason === 'unsupported') holdOffUntil = Date.now() + HOLD_RETRY_MS;
        started(false);
        return;
      }
      let first = true;
      const drive = held.consume((message, meta) => {
        // The submit's log on the first run only: a retry in place reads afresh.
        const extra: Pick<DeliverMeta, 'log' | 'held'> = { held: true, ...(first ? { log } : {}) };
        first = false;
        return consumeWith(message, meta, extra);
      });
      started(true);
      await drive;
    })();
    void done.catch(() => started(false));
    const entry: { done: Promise<void>; settledAt?: number } = { done };
    void done.then(
      () => (entry.settledAt = Date.now()),
      () => (entry.settledAt = Date.now())
    );
    const at = Date.now();
    for (const [k, v] of paired) if (v.settledAt !== undefined && at - v.settledAt > RECENT_LOG_MS) paired.delete(k);
    paired.set(sessionId, entry);
    return { sent, done };
  };

  /**
   * What submits just wrote, by session, for a drive that follows at once
   * (see `drive`): the log, and the deadline anchor its companion stands
   * behind.
   */
  const recent = new Map<string, { log: SessionLog; at: number; anchor?: Anchor; backstopped?: boolean }>();
  const remember = (sessionId: string, entry: { log: SessionLog; anchor?: Anchor; backstopped?: boolean }) => {
    const at = Date.now();
    for (const [k, v] of recent) if (at - v.at > RECENT_LOG_MS) recent.delete(k);
    recent.set(sessionId, { ...entry, at });
  };
  /**
   * A deadline anchor for a drive about to start in this process, and the
   * companion that stands behind it: delayed past the deadline, keyed so a
   * submission's companion is sent once.
   */
  const directAnchor = async (sessionId: string, key: string) => {
    const at = await directDeadline(world, options);
    const { anchor, companion } = policy.direct(at);
    const delaySeconds = Math.max(1, Math.ceil((at - Date.now()) / 1000)) + 5;
    const send = () => wake(world, sessionId, key, delaySeconds, { companion: true });
    return { anchor, companion: companion ? send : undefined };
  };

  /**
   * The last log a submit read per session in this process, so the next
   * submit reads only what was appended since (one short page) instead of
   * the whole log, which grows with every turn and was most of a submit's
   * time on a long session. A log with a read or write in flight is not
   * reused: a submit must not queue behind a slow write of an earlier drive.
   */
  const logs = new Map<string, SessionLog>();
  const submitLog = (sessionId: string): SessionLog => {
    const cached = logs.get(sessionId);
    if (cached && !cached.busy && !cached.isFenced) return cached;
    return new SessionLog(world, sessionId);
  };
  const rememberLog = (sessionId: string, log: SessionLog) => {
    logs.delete(sessionId);
    logs.set(sessionId, log);
    // Bounded: the oldest sessions fall out.
    while (logs.size > MAX_CACHED_LOGS) logs.delete(logs.keys().next().value as string);
  };

  /**
   * The session's docs hook, created on the first out-of-band edit. Two
   * first edits at once both write a `hook_created` for the one token: the
   * World answers the second with a `hook_conflict`, and the first stands.
   */
  async function docsHook(log: SessionLog): Promise<{ hookId: string; token: string }> {
    if (log.state.docsHook) return log.state.docsHook;
    let failed: unknown;
    try {
      // Not a webhook: a World treats a hook that does not say so as public,
      // resumable by its (guessable) token through the webhook route.
      await log.write({ eventType: 'hook_created', correlationId: `hook_${ulid()}`, eventData: { token: docsHookToken(log.runId), isWebhook: false } });
    } catch (err) {
      failed = err;
    }
    // Ours, or (a conflict, a lost answer) someone's that this log has not
    // folded yet: written once, then looked for.
    for (let attempt = 0; !log.state.docsHook; attempt++) {
      if (attempt >= 5) throw failed instanceof Error ? failed : new Error(`harness-world: could not create ${log.runId}'s docs hook`);
      if (attempt > 0) await new Promise((r) => setTimeout(r, 50 * attempt));
      await log.refresh().catch(() => {});
    }
    return log.state.docsHook;
  }

  async function updateDoc<T extends object>(sessionId: string, doc: Doc<T>, mutate: (draft: T) => void | T, updateOptions: UpdateDocOptions = {}): Promise<DocSnapshot<T>> {
    const def = doc?.definition;
    if (!def) throw new TypeError('harness-world: updateDoc needs a document token (defineDoc)');
    const { expectRevision } = updateOptions;
    const retries = expectRevision !== undefined ? 0 : Math.max(0, updateOptions.retries ?? 5);
    let lastRevision = 0;
    for (let round = 0; round <= retries; round++) {
      const log = submitLog(sessionId);
      await log.refresh();
      rememberLog(sessionId, log);
      if (log.state.status !== 'running') {
        throw new Error(`harness-world: ${sessionId} is ${log.state.status}; its documents are read-only`);
      }
      const base = materializeDoc(def, log.state.docs.get(def.kind));
      lastRevision = base.revision;
      if (expectRevision !== undefined && expectRevision !== base.revision) throw new DocConflict(def.kind, base.revision, expectRevision);
      const edit = encodeEdit(def, base, prepareUpdate(def, base.value, mutate, { storedBytes: () => storedBytes(base) }), { exact: true });
      if (!edit) return { value: base.value as unknown as DeepReadonly<T>, revision: base.revision };
      const hook = await docsHook(log);
      const id = `dedit_${ulid()}`;
      const request = {
        eventType: 'hook_received' as const,
        correlationId: hook.hookId,
        eventData: { token: hook.token, payload: encode({ $docs: 1, id, docs: [edit] } satisfies DocsPayload) },
      };
      // A write whose answer was lost may have landed: look its id up
      // before sending it again (a resend with the same id is inert anyway).
      for (let attempt = 1; !log.state.docEdits.has(id); attempt++) {
        try {
          await log.write(request);
        } catch (err) {
          if (!isAmbiguous(err) || attempt >= 3) throw err;
          options.log?.('updateDoc: ambiguous write, looking it up', { runId: sessionId, kind: def.kind, error: String(err) });
          await log.refresh();
          continue;
        }
        if (!log.state.docEdits.has(id)) await log.refresh();
        if (!log.state.docEdits.has(id)) throw new Error(`harness-world: ${sessionId}: an edit of ${def.kind} was written but is not in the log`);
      }
      const outcome = log.state.docEdits.get(id)!;
      const after = log.state.docs.get(def.kind);
      lastRevision = after?.revision ?? 0;
      if (outcome.applied && after) {
        const out = new JsonLinesWriter(world, sessionId, liveStreamName(sessionId));
        out.write(docLiveEvent(after, markTime(), def.live));
        const flushed = out.flush().catch((err) => options.log?.('updateDoc: the live doc event was lost', { runId: sessionId, error: String(err) }));
        if (updateOptions.waitUntil) updateOptions.waitUntil(flushed);
        else await flushed;
        return readDoc(log.state, doc);
      }
      if (expectRevision !== undefined) throw new DocConflict(def.kind, lastRevision, expectRevision);
      options.log?.('updateDoc: lost a race, reading again', { runId: sessionId, kind: def.kind, revision: lastRevision });
      // Writers that keep colliding spread out.
      if (round < retries) await new Promise((r) => setTimeout(r, Math.random() * 25 * (round + 1)));
    }
    throw new DocConflict(def.kind, lastRevision);
  }

  async function submit(
    sessionId: string,
    payload: SubmissionPayload,
    sendOptions: SendOptions = {}
  ): Promise<string> {
    const t0 = markTime();
    const submitInfo = processInfo('submit');
    const log = submitLog(sessionId);
    await log.refresh();
    rememberLog(sessionId, log);
    const identity = sendOptions.identity;
    if (identity !== undefined) {
      if (typeof identity !== 'string' || identity.length === 0) {
        throw new TypeError('harness-world: identity must be a nonempty string');
      }
      if (runAheadFor(options, log.state)) {
        throw new Error('harness-world: caller identity requires runAhead=false');
      }
      payload = { ...payload, nonce: `caller:${identity}` };
    }
    const nonce = payload.nonce ?? ulid();
    const canonical = () => {
      const submission = log.state.submissions.find((s) => s.nonce === nonce);
      if (!submission) return undefined;
      if (identity !== undefined && (submission.kind !== payload.kind || submission.text !== payload.text)) {
        throw new SubmissionConflict(sessionId, identity, submission.id);
      }
      return submission.id;
    };
    let existing: string | undefined;
    if (identity !== undefined) existing = canonical();
    const inbox = log.state.inbox;
    if (!inbox) throw new Error(`harness-world: ${sessionId} has no inbox`);
    const t1 = markTime();
    // Send-and-hold in place of the deadline and its companion (see
    // `SendOptions.wake`). A chaos submission arms a crash for a drive that
    // comes later: it wakes nothing now.
    const held = sendOptions.wake === 'backstop' && holds && payload.kind !== 'chaos' && Date.now() >= holdOffUntil;
    // Run ahead: when the drive follows in this process (the
    // drive route), the submission goes into the log's pipeline with a
    // client-minted id (`cid`) and the drive places it at once; it becomes
    // durable behind it. A resend is inert by `cid`; a lost response is
    // looked up by `cid` before anything is resent.
    if (sendOptions.wake === 'backstop' && runAheadFor(options, log.state) && payload.kind !== 'withdraw') {
      const cid = `sub_${nonce}`;
      const pipe = PipelinedLog.for(log, { ...(options.log ? { log: options.log } : {}) });
      const durable = pipe.enqueue(
        {
          eventType: 'hook_received',
          correlationId: inbox.hookId,
          eventData: { token: inbox.token, payload: encode({ ...payload, nonce, cid }) },
        },
        {
          landed: (st) => st.submissions.some((x) => x.id === cid),
          // Not a view of anything: it lands whatever else the pipeline
          // rejects. A blip is resent (by `cid`, idempotent) for the
          // demotion window, and a write still lost after that is re-issued
          // by the drive's restart rather than dropped.
          survives: true,
          persistMs: RUN_AHEAD_DEMOTE_MS,
          reissue: true,
        }
      );
      // A delivery stands behind the prompt from the start, not from the
      // ack: if this process dies, or the write is lost, a companion still
      // comes, past the drive's deadline. The key is per cid, so it is sent
      // once; it drops once the turn is answered.
      // Held: the wake itself, undelayed, under the same key. Its drive here
      // starts from this log, so it places the submission from the pipeline.
      let armed: Promise<unknown>;
      if (held) {
        const hold = sendHeld(sessionId, `${sessionId}:${cid}`, log);
        // Not held, the queue's delivery may come before the submission is
        // durable, find nothing to do and ack: one more wake once it is.
        const landed = hold.sent.then((wasHeld) => (wasHeld ? undefined : durable.then(() => wake(world, sessionId, `${sessionId}:${cid}:durable`), () => {})));
        armed = Promise.all([hold.done, landed]);
      } else {
        const direct = await directAnchor(sessionId, `${sessionId}:${cid}`);
        armed = direct.companion?.() ?? Promise.resolve();
        remember(sessionId, { log, anchor: direct.anchor, backstopped: !!direct.companion });
      }
      const live = marksEnabled(options) ? new JsonLinesWriter(world, sessionId, liveStreamName(sessionId)) : undefined;
      const after = durable.then(async () => {
        live?.write(mark(sessionId, 'submit', { id: cid, kind: payload.kind, durableSlot: log.state.eventCount, readMs: Math.round(t1 - t0), lagMs: Math.round(markTime() - t1), ahead: true, ...submitInfo }));
        await Promise.all([
          live?.flush(),
          publishHint(world, sessionId, { eventType: 'hook_received', slot: log.state.eventCount, id: cid }),
        ]);
      });
      const background = Promise.all([
        armed,
        after.catch(async (err) => {
          options.log?.('submit: run-ahead write failed', { runId: sessionId, error: String(err) });
          // Say so where the client reads: the speculative answer to this
          // prompt is not durable (yet). The drive's restart re-issues it.
          const out = new JsonLinesWriter(world, sessionId, liveStreamName(sessionId));
          out.write({ type: 'submit-failed', id: cid, error: String(err), reissued: true, t: markTime() });
          await out.flush().catch(() => {});
        }),
      ]).catch((err) => options.log?.('submit: run-ahead wake failed', { runId: sessionId, error: String(err) }));
      if (sendOptions.waitUntil) sendOptions.waitUntil(background);
      else void background;
      return cid;
    }
    const encoded = encode({ ...payload, nonce });
    let resume: Required<Pick<CreateEventParams, 'resumeId' | 'resumePayloadDigest'>> | undefined;
    if (identity !== undefined && existing === undefined) {
      let supported = world.capabilities?.hookResumeDedup === true;
      if (!supported) {
        // Attestation is response-only: never cache it across submissions.
        const hook = await world.hooks.getByToken(inbox.token);
        if (hook.runId !== sessionId || hook.hookId !== inbox.hookId) {
          throw new Error('harness-world: inbox ownership changed');
        }
        supported = (hook.resumeCapabilities?.hookResumeDedupVersion ?? 0) >= HOOK_RESUME_DEDUP_VERSION;
      }
      if (supported) {
        const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', new Uint8Array(encoded)));
        resume = {
          resumeId: nonce,
          resumePayloadDigest: Array.from(digest, (byte) => byte.toString(16).padStart(2, '0')).join(''),
        };
      }
    }
    const write = () =>
      log.write({
        eventType: 'hook_received',
        correlationId: inbox.hookId,
        eventData: { token: inbox.token, payload: encoded },
      }, { resume });
    let id: string;
    if (existing !== undefined) {
      id = existing;
    } else {
      try {
        const { event } = await write();
        id = event?.eventId ?? `unknown_${ulid()}`;
      } catch (error) {
        // World owns atomic transport retries. Reconcile after failure rather
        // than stacking a resend on its retry loop; ordinary writes retain
        // their existing lookup-before-resend behavior.
        if (identity !== undefined || isAmbiguous(error)) {
          if (isAmbiguous(error)) options.log?.('submit: ambiguous write, looking it up', { runId: sessionId, error: String(error) });
          await log.refresh();
          const landed = canonical();
          if (landed !== undefined) id = landed;
          else if (isAmbiguous(error) && !resume) {
            const { event } = await write();
            id = event?.eventId ?? `unknown_${ulid()}`;
          } else throw error;
        } else throw error;
      }
    }
    // A stale append can return an older canonical event, or a fallback
    // World can append a duplicate. The folded winner owns the identity.
    if (identity !== undefined) {
      const accepted = canonical();
      if (accepted === undefined) throw new Error('harness-world: submission identity was not folded');
      id = accepted;
    }
    // A driver waiting on this session's inbox reads now, not at its next poll.
    const hinted = publishHint(world, sessionId, { eventType: 'hook_received', slot: log.state.eventCount, id });
    // Timing (marks.ts): when the submission became durable, what it cost.
    const live = marksEnabled(options) ? new JsonLinesWriter(world, sessionId, liveStreamName(sessionId)) : undefined;
    live?.write(
      mark(sessionId, 'submit', {
        id,
        kind: payload.kind,
        durableSlot: log.state.eventCount,
        readMs: Math.round(t1 - t0),
        lagMs: Math.round(markTime() - t1),
        ...submitInfo,
      })
    );
    // Everything after the commit runs concurrently. Event ids are slots,
    // unique per run only: the wake key must name the run.
    let woken: Promise<void>;
    /** Held: once the queue answered (the drive itself is `woken`, not awaited by `send`). */
    let answered: Promise<unknown> | undefined;
    if (held) {
      // The wake itself, undelayed and held, under the key the companion
      // would have had; its drive here starts from the log this submit wrote.
      const hold = sendHeld(sessionId, `${sessionId}:${id}`, log);
      woken = hold.done;
      answered = hold.sent;
    } else if (sendOptions.wake === 'backstop') {
      // The log this submit just wrote, and the deadline its companion
      // stands behind: a drive of this session in this process starts from
      // them instead of listing the log again.
      const direct = await directAnchor(sessionId, `${sessionId}:${id}`);
      remember(sessionId, { log, anchor: direct.anchor, backstopped: !!direct.companion });
      woken = direct.companion?.() ?? Promise.resolve();
    } else {
      woken = wake(world, sessionId, `${sessionId}:${id}`);
    }
    const rest = Promise.all([live?.flush(), hinted]);
    if (sendOptions.waitUntil) {
      // The caller drives (or answers) now. A queue wake is what drives the
      // session, so it is still awaited; a backstop companion is not.
      sendOptions.waitUntil(
        Promise.all([rest, sendOptions.wake === 'backstop' ? woken : undefined]).catch((err) =>
          options.log?.('submit: background work failed', { runId: sessionId, error: String(err) })
        )
      );
      if (sendOptions.wake !== 'backstop') await woken;
    } else if (answered) {
      // Not the held drive: that is the turn (the `drive()` after this waits for it).
      await Promise.all([answered, rest]);
    } else {
      await Promise.all([woken, rest]);
    }
    return id;
  }

  return {
    handler,
    consume,

    async start(startOptions = {}) {
      const harness = startOptions.harness
        ? harnesses.find((h) => h.name === startOptions.harness)
        : harnesses[0];
      if (!harness) throw new Error(`harness-world: unknown harness "${startOptions.harness}"`);
      const workflowName = workflowNameFor(harness.name);
      const runId = `wrun_${ulid()}`;
      const deploymentId = await world.getDeploymentId();
      await world.events.create(runId, {
        eventType: 'run_created',
        specVersion: world.specVersion,
        eventData: {
          deploymentId,
          workflowName,
          // A World's restart recovery re-enqueues the run on this namespace
          // (the kernel's queue), in whichever process recovers it.
          executionContext: { ...EXECUTION_CONTEXT },
          input: encode({
            harness: harness.name,
            config: startOptions.config,
          } satisfies RunEnvelope),
        },
      });
      const log = new SessionLog(world, runId);
      await log.refresh();
      await log.write({ eventType: 'run_started' });
      await log.write({
        eventType: 'hook_created',
        correlationId: `hook_${ulid()}`,
        // Not a webhook: Worlds (world-vercel, world-postgres) treat a hook
        // that does not say so as public, and this token is guessable from
        // the session id, so the Workflow SDK's webhook route would let anyone
        // who knows a session id write into its inbox.
        eventData: { token: `harness:${runId}:inbox`, isWebhook: false },
      });
      if (startOptions.prompt) {
        await submit(runId, { kind: 'prompt', text: startOptions.prompt }, startOptions.wake ? { wake: startOptions.wake } : {});
      }
      return { sessionId: runId };
    },

    send: (sessionId, text, kind = 'prompt', sendOptions) =>
      submit(sessionId, { kind, text }, sendOptions),

    drive: async (sessionId, meta = {}) => {
      const fresh = recent.get(sessionId);
      recent.delete(sessionId);
      // The submit before this sent its wake held: that message's delivery
      // in this process is this drive (or, not held, the queue's).
      const heldDrive = meta.anchor ? undefined : takePaired(sessionId);
      if (heldDrive) {
        await heldDrive;
        return undefined;
      }
      const usable = fresh && Date.now() - fresh.at < RECENT_LOG_MS ? fresh : undefined;
      const log = meta.log ?? usable?.log;
      let anchor = meta.anchor ?? usable?.anchor;
      // Whether a companion stands behind the drive (`DeliverMeta.backstopped`).
      let backstopped = meta.anchor ? meta.backstopped : usable?.backstopped;
      if (!anchor) {
        // No queue message holds this drive: declare a deadline, and stand
        // a companion behind it before driving.
        const direct = await directAnchor(sessionId, `${sessionId}:direct:${ulid()}`);
        anchor = direct.anchor;
        await direct.companion?.();
        backstopped = !!direct.companion;
      }
      return deliver(options, sessionId, { ...meta, anchor, ...(backstopped ? { backstopped: true } : {}), ...(log ? { log } : {}) });
    },

    chaos: (sessionId, spec, options) => submit(sessionId, { kind: 'chaos', chaos: spec }, options ?? {}),

    async withdraw(sessionId, submissionId) {
      await submit(sessionId, { kind: 'withdraw', target: submissionId });
      const log = new SessionLog(world, sessionId);
      await log.refresh();
      return log.state.withdrawn.has(submissionId)
        ? 'withdrawn'
        : 'already_placed';
    },

    async close(sessionId) {
      const log = new SessionLog(world, sessionId);
      await log.refresh();
      await log.write({
        eventType: 'run_completed',
        eventData: { output: encode(null) },
      });
      // Over: its streams end, so their readers stop waiting.
      await closeRunStreams(world, sessionId, kernelStreamNames(sessionId));
    },

    async inspect(sessionId) {
      const log = new SessionLog(world, sessionId);
      await log.refresh();
      return log.state;
    },

    async doc(sessionId, doc) {
      const log = submitLog(sessionId);
      await log.refresh();
      rememberLog(sessionId, log);
      return readDoc(log.state, doc);
    },

    updateDoc,

    async liveEnd(sessionId) {
      try {
        return (await world.streams.getInfo(sessionId, liveStreamName(sessionId))).tailIndex + 1;
      } catch {
        return 0; // Nothing written yet.
      }
    },

    live: (sessionId, liveOptions) =>
      tailJsonLines(world, sessionId, liveStreamName(sessionId), liveOptions),
    liveEntries: (sessionId, liveOptions) =>
      tailJsonLinesAt(world, sessionId, liveStreamName(sessionId), liveOptions),
  };
}

/**
 * Whether a failed write may still have taken effect: a transport failure
 * (the relay's `RelayTransportError`, a network `TypeError`), not an answer.
 */
export function isAmbiguous(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false;
  if ((error as { ambiguous?: unknown }).ambiguous === true) return true;
  return error instanceof TypeError && /fetch|network|load failed/i.test(error.message);
}

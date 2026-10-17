import type { World } from '@workflow/world';
import { tracedDrive } from './agent-runs.ts';
import { monotonicFactory } from 'ulid';
import { encode } from './codec.ts';
import type { StepEnvelope } from './fold.ts';
import { pendingSubmissions } from './fold.ts';
import { callDocs, type DocDefinition, type DocLive, type DocState, docLiveEvent, docsReader, outcomeResult } from './docs.ts';
import {
  armedChaos,
  ATTACH_DRILLS,
  type ChaosKind,
  type ChaosOutput,
  type ChaosSpec,
  chaosEnabledByEnv,
  chaosKey,
  chaosKind,
  chaosMatches,
  chaosOutputOf,
  crashedByChaos,
  crashProcess,
  DEFAULT_CHAOS_AFTER_MS,
  type Drill,
  type DrillTarget,
  isSessionDrill,
} from './chaos.ts';
import { commitClose, commitDetach, commitFact, finishFact, isDetached, isFactStep, settleParked } from './facts.ts';
import { armChaosFire, armOwnerCheck, type OwnerPolicy } from './owners.ts';
import { currentLane, type HandoffOutput, homeLane, isEpochStep, type LaneOptions, needsOf, route } from './lanes.ts';
import {
  type ActiveDrive,
  activeDrive,
  type Anchor,
  anchorKey,
  type AnchorPolicy,
  anchorVerdict,
  claimDrive,
  defaultAnchors,
  driveSettled,
  dropDrive,
  ReleaseAnchor,
  releaseDrive,
  type VerdictContext,
} from './anchor.ts';
import { harnessOf, stepNameFor } from './names.ts';
import { type Mark, type MarkName, mark, markTime, PROCESS_ID, processInfo } from './marks.ts';
import { LogFenced, LogSuperseded, SessionLog, type WriteResult } from './session-log.ts';
import { PipelinedLog, type RejectCause, Rejection, stepKnown } from './pipelined-log.ts';
import { type Hint, publishHint, watchHints } from './hints.ts';
import { commitUnavailable, defaultHint, failingOf, lastUnavailable, MAX_FAILED_DELIVERIES, type UnavailableInfo, unavailableOf } from './unavailable.ts';
import {
  closeRunStreams,
  effectStreamName,
  JsonLinesWriter,
  kernelStreamNames,
  liveStreamName,
  readJsonLines,
} from './streams.ts';
import type { CreateEventRequest, Event } from '@workflow/world';
import type {
  DriveContext,
  EffectError,
  EffectIO,
  EffectOptions,
  EffectRecord,
  Harness,
  Lane,
  ParkHandle,
  ParkOptions,
  ParkOwner,
  SessionState,
  Submission,
} from './types.ts';

export { QUEUE_NAME, QUEUE_PREFIX } from './queue.ts';
import { QUEUE_NAME } from './queue.ts';
export { EffectFailure, EffectInterrupted, Fatal, Parked, Restart, Yield } from './errors.ts';
import { EffectFailure, EffectInterrupted, Fatal, Parked, Restart, Yield } from './errors.ts';

const ulid = monotonicFactory();

export interface DriverOptions {
  world: World;
  /**
   * Hold the World's per-run event channel (world-vercel's WebSocket) open
   * for this delivery, so its writes go over one socket instead of one HTTP
   * request each. Returns the release. The queue handler already does this
   * for flow-route deliveries; drivers invoked any other way (a drive route,
   * a daemon in a sandbox) pass it. See `vercel-channel.ts`.
   */
  openChannel?: (runId: string) => (() => void) | undefined;
  /** One harness, or several: a session is driven by the one it was started with. */
  harness: Harness | Harness[];
  /**
   * How this World holds its deliveries (anchor.ts): what anchors a
   * delivery, and what the World can prove about a holder this process
   * cannot see. Liveness is the queue's alone: a delivery that finds open
   * work held elsewhere acks, because that holder takes it if it lives and
   * its message comes back if it died. Default `defaultAnchors()`: the
   * message lease on Vercel, the process elsewhere in Node.
   */
  anchors?: AnchorPolicy;
  /**
   * The longest a drive that holds no queue message (the drive route,
   * `sessions.drive()`) runs before it hands over to the queue. Its deadline
   * is its anchor: a queue message delayed past it stands behind it, so a
   * drive that dies silently is recovered at most this long (plus ~5s)
   * after it started. Default 60s.
   */
  directDriveMs?: number;
  /** A write unanswered this long makes the drive give its delivery up (see `WRITE_STALL_MS`, the default). */
  writeStallMs?: number;
  /** Upper bound on one invocation when the World does not report a deadline. */
  maxInvocationMs?: number;
  /** Margin kept before the deadline: no new effects start inside it. */
  deadlineMarginMs?: number;
  /** Redelivery delay after yielding to another live driver. */
  contendedDelaySeconds?: number;
  /**
   * Called with the fold before this driver touches the session. Lets a
   * deployment run one session across several lanes (a browser tab, a
   * function, ...): a lane that does not currently own the session declines,
   * optionally asking to be woken again later (a new queue message). Default:
   * always admit.
   */
  admit?: (
    state: SessionState
  ) => Admission | Promise<Admission>;
  /**
   * Multi-lane sessions (see `LaneOptions`). A harness's own `lanes` take
   * precedence, so one driver can host lane-hopping sessions next to
   * ordinary ones.
   */
  lanes?: LaneOptions;
  /**
   * Honor armed crashes (`chaos` submissions, see chaos.ts): kill this
   * process for real partway through a matching effect. Off unless set, or
   * unless `HARNESS_CHAOS=1` in the environment. `true` is `{ fire: true }`.
   */
  chaos?: boolean | ChaosDriveOptions;
  /** How a chaos crash kills this invocation. Default: exit the process (or reload the tab). */
  crash?: () => void;
  /**
   * Who polices parked intents (see owners.ts), by `ParkOwner.kind`. Used by
   * owner checks in the process whose queue is durable.
   */
  owners?: OwnerPolicy[];
  log?: (message: string, fields?: Record<string, unknown>) => void;
  /**
   * Offer drives live hints (hints.ts): `ctx.onHint` fires when another
   * process commits a submission or moves the session, so a harness can
   * read then instead of polling. Default on; `false` (or
   * `HARNESS_HINTS=0`) leaves `ctx.onHint` undefined and harnesses poll.
   * (A drive always listens for epoch hints itself, to stop at once when
   * another delivery takes the session over.)
   */
  hints?: boolean;
  /**
   * Write timing marks (marks.ts) to the `live` stream. Default on, unless
   * `HARNESS_MARKS=0` in the environment.
   */
  marks?: boolean;
  /**
   * Mark a drive's writes in-band and fence them with the World's in-band
   * writer fence (session-log.ts `arm`): a write
   * decided from a stale log is refused by the World instead of landing.
   * Default: on wherever the World declares `capabilities.inBandFence` (and
   * `HARNESS_IN_BAND_FENCE` is not `0`); `false` keeps every write unfenced.
   * A World without the fence is never sent it.
   */
  inBandFence?: boolean;
  /**
   * Run-ahead durability (pipelined-log.ts): drives write through
   * a `PipelinedLog` and wait for acks only at barriers (before an effect
   * that must not run twice, before giving up the session). Default: the
   * session's own choice (`config.durability === 'run-ahead'` on
   * `run_created`), else `HARNESS_DURABILITY=run-ahead` in the environment.
   */
  runAhead?: boolean;
  /**
   * A drive whose harness cannot run here (`Unavailable`, unavailable.ts: a
   * missing binary, a module that is not installed) fails its turn instead
   * of ending the delivery with the error: an `unavailable` fact takes the
   * turn's messages, the live stream gets an `error` and an `idle`, and the
   * session stays running for the next message (a direct drive with a queue
   * message behind it hands the turn to the queue first: see
   * `DeliverMeta.backstopped`). This says what to do about
   * it (the live event's `hint`); undefined leaves it to the error's own
   * hint, else `defaultHint`.
   */
  unavailableHint?: (info: UnavailableInfo, harness: string) => string | undefined;
  /**
   * The delivery of one message at which an error the driver does not
   * recognize fails the turn too (code `failing`), instead of being
   * redelivered again: every earlier delivery of that message failed or
   * died. World and network errors never count. Default 8 (on Vercel's
   * queue, a few minutes of backoff); `Infinity` keeps redelivering.
   */
  maxFailedDeliveries?: number;
}

/** Whether drives of this session run ahead of durability (see `DriverOptions.runAhead`). */
export function runAheadFor(options: Pick<DriverOptions, 'runAhead'>, state: SessionState): boolean {
  if (options.runAhead !== undefined) return options.runAhead;
  const config = state.config as { durability?: unknown } | undefined;
  if (config && typeof config === 'object' && config.durability !== undefined) return config.durability === 'run-ahead';
  return globalThis.process?.env?.HARNESS_DURABILITY === 'run-ahead';
}

/** The anchor policy a driver runs with (see `DriverOptions.anchors`). */
export function anchorsOf(options: Pick<DriverOptions, 'anchors'>): AnchorPolicy {
  return options.anchors ?? defaultAnchors();
}

/** See `DriverOptions.directDriveMs`. */
export const DEFAULT_DIRECT_DRIVE_MS = 60_000;

/**
 * A run-ahead drive whose oldest unacked write is this old gives its
 * delivery up (self-demotion): it fences itself and hands the session over
 * through a continuation, whose delivery fences the epoch, so the demoted
 * drive's queued writes come back fenced rather than racing the new owner.
 * Never while a `replay: 'never'` effect of it is in `fn`
 * (`PipelinedLog.liveHeld`): a recovery would mark it interrupted while it
 * runs.
 */
export const RUN_AHEAD_DEMOTE_MS = 5000;

/**
 * A write unanswered this long (and not found committed by the slow-write
 * probe) makes the drive give its delivery up the same way: the queue
 * library keeps renewing a delivery whose event loop runs, so a drive stuck
 * on a hung write would otherwise hold the session until its deadline.
 */
export const WRITE_STALL_MS = 30_000;

/**
 * How often a drive re-reads the log when nothing else made it read
 * (a harness that never polls, a lost epoch hint): a read, so a preempted
 * drive stops within this long. It writes nothing.
 */
export const PREEMPTION_REFRESH_MS = 15_000;

/** The least time a budget yield spends finishing up before it wakes the session, whatever the margin. */
const BUDGET_FINISH_FLOOR_MS = 500;

/** How long a superseded drive waits for its aborted effects to stop before it hands its open work on. */
const SUPERSEDED_SETTLE_MS = 5000;

/**
 * Re-issue writes the pipeline could not land (`reissue`: outcomes the
 * harness already acted on), before a restart replays without them.
 * Returns whether there were any.
 */
async function reissueOrphans(pipe: PipelinedLog): Promise<boolean> {
  const orphans = pipe.takeOrphans();
  for (const o of orphans) {
    await pipe.enqueue(o.req, { ...o.opts, reissue: false, survives: true }).catch(() => {});
  }
  return orphans.length > 0;
}

/** Whether a driver writes timing marks. */
export function marksEnabled(options: Pick<DriverOptions, 'marks'>): boolean {
  if (options.marks !== undefined) return options.marks;
  const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env;
  return env?.HARNESS_MARKS !== '0';
}

/** Write a mark for `log`'s run to `live`, stamped with the log's durable position. */
export function writeMark(
  live: JsonLinesWriter,
  log: SessionLog,
  name: MarkName,
  fields: Partial<Omit<Mark, 'type' | 'name' | 'runId'>> = {}
): void {
  live.write(mark(log.runId, name, { durableSlot: log.state.eventCount, ...fields }));
}

export type Admission =
  | { admit: true }
  | { admit: false; reason: string; timeoutSeconds?: number };

/** What the queue told a delivery about itself. */
export interface DeliverMeta {
  /**
   * What holds this delivery (anchor.ts): its queue message (the queue
   * handler fills this in), or, for a drive that holds none, a deadline.
   * Default: the policy's direct anchor (`AnchorPolicy.direct`), with no
   * queue message behind it; `sessions.drive()` sends one where the World
   * needs it.
   */
  anchor?: Anchor;
  /**
   * The anchor (`anchorKey`) a continuation hands over: the delivery that
   * sent it stopped with open work and gave it to this one as its last act.
   */
  continues?: string;
  /** The queue's delivery count for this message: above 1 means the last delivery of it did not ack. */
  attempt?: number;
  /**
   * This delivery stands behind a direct drive (`SendOptions.wake:
   * 'backstop'`, the drive route), delayed past that drive's deadline.
   */
  companion?: boolean;
  /**
   * The submission a companion stands behind (from its wake key). Once that
   * submission is placed, nothing is running and a newer submission waits,
   * the companion acks and drops instead of driving: the newer one has a
   * wake of its own, whose direct drive it must not race.
   */
  guards?: string;
  /**
   * A queue message (its companion) stands behind this direct drive, delayed
   * past its deadline (`sessions.drive()` sets this when it sent one, or the
   * submit before it did). Such a drive whose harness cannot run here
   * (unavailable.ts) does not fail the turn: it hands the turn to the queue
   * at once (a continuation), because the Function that serves the queue may
   * carry what this one lacks (another bundle). Only a queue delivery that
   * meets it fails the turn.
   */
  backstopped?: boolean;
  /**
   * A log of this session the caller just refreshed or wrote (the drive
   * route hands over the one its submit used). The delivery starts from it
   * instead of listing the log again; it is refreshed incrementally later.
   */
  log?: SessionLog;
}

/**
 * Handle one queue delivery for one session: rebuild from the log, recover
 * open work whose holder is provably gone, and drive the harness until it is
 * idle or out of budget.
 *
 * One drive per session per process: a delivery that finds this session
 * already driving here acks, and that drive runs once more before it lets go
 * (it may be past its last read). Liveness is the queue's (anchor.ts):
 * nothing here writes a heartbeat, and nothing asks the queue to come back
 * to look again; a delivery that wants another later sends a new message.
 * Always resolves `undefined` (the return type is kept for queue routes).
 */
export async function deliver(
  driverOptions: DriverOptions,
  runId: string,
  meta: DeliverMeta = {}
): Promise<{ timeoutSeconds: number } | undefined> {
  const policy = anchorsOf(driverOptions);
  const { world } = driverOptions;
  const say = driverOptions.log ?? (() => {});
  const anchor = meta.anchor ?? policy.direct(Date.now() + (driverOptions.directDriveMs ?? DEFAULT_DIRECT_DRIVE_MS)).anchor;
  const key = driverKey(driverOptions);
  // A delivery that carries a grant (its own message redelivered, or a
  // continuation) is the only one that may take the work it was granted: a
  // drive already under way here judges that work by its own anchor, finds
  // it covered, and acks, and nothing would ever come back for it. So it is
  // never folded into that drive's rerun; it waits for it instead.
  const granted = meta.continues !== undefined || (meta.attempt ?? 1) > 1;
  let claim = claimDrive(world, runId, { drv: '', anchor, key });
  if (!claim) {
    const current = activeDrive(world, runId);
    if (!granted && (!current || current.key === key)) {
      say('skip: this session is driving in this process; it drives once more', { runId });
      return undefined;
    }
    // Another driver of this process (other lanes: a tab handing off to a
    // daemon it shares a World with, in tests) may not serve the lane the
    // session is in now, so its rerun is no promise: look after it.
    say(
      granted
        ? 'wait: this session drives here; a delivery with a grant runs after it'
        : 'wait: this session drives here under another driver; looking after it',
      { runId }
    );
    for (;;) {
      await driveSettled(world, runId);
      claim = claimDrive(world, runId, { drv: '', anchor, key });
      if (claim) break;
      if (!granted) {
        say('skip: this session is driving in this process; it drives once more', { runId });
        return undefined;
      }
    }
  }
  const releaseChannel = driverOptions.openChannel?.(runId);
  let unserved = false;
  try {
    let round: DeliverMeta = { ...meta, anchor };
    let refusals = 0;
    for (let i = 0; ; i++) {
      claim.rerun = false;
      const outcome = await deliverOpen(driverOptions, policy, runId, round, claim, refusals);
      if (outcome === 'refused') {
        // The World refused this delivery's first in-band write (its recovery,
        // or the takeover's first resolution) and nobody moved the session:
        // the log moved under its decision. Decide again from a fresh load,
        // with the same grant.
        refusals++;
        say('in-band fence: decided from a stale log; deciding again', { runId, refusals });
        round = { ...meta, anchor, log: undefined };
        continue;
      }
      // Handed to the queue (a direct drive whose harness cannot run here):
      // the continuation it sent takes every look a delivery asked for since.
      if (outcome === 'handed') break;
      // Checked and released with no await in between: a delivery that
      // lands after this either finds the drive gone or is seen here.
      if (!claim.rerun) break;
      if (i >= MAX_RERUNS) {
        unserved = true;
        break;
      }
      say('rerun: a delivery landed while this drove', { runId });
      // A fresh look, under the same anchor: nothing to hand over or fence.
      round = { anchor, ...(meta.backstopped ? { backstopped: true } : {}) };
    }
  } finally {
    releaseDrive(world, runId, claim);
    releaseChannel?.();
  }
  // The deliveries that asked for one more look acked on this drive's word:
  // past the rerun cap, that look goes out as a message of its own.
  if (unserved) await wake(world, runId, `${runId}:rerun:${anchorKey(anchor)}:${ulid()}`);
  return undefined;
}

const MAX_RERUNS = 8;

/** Which lanes and harnesses a driver serves: drives with the same key can take each other's reruns. */
function driverKey(options: DriverOptions): string {
  const harnesses = [options.harness].flat();
  return JSON.stringify([options.lanes?.serves ?? null, harnesses.map((h) => [h.name, h.lanes?.serves ?? null])]);
}

/** Whether there is anything for a drive to do (a wake for any of these must not be dropped). */
export function workRemains(state: SessionState): boolean {
  return workFingerprint(state) !== '';
}

/**
 * What work is waiting: pending submissions, parked steps that settled
 * (a drive re-enters them), and released intents (`pending`, a retry the
 * harness re-requests). Empty when there is none. A turn an `unavailable`
 * fact failed is over (unavailable.ts): what it left released or settled
 * is nobody's to re-enter, so it is not work.
 */
function workFingerprint(state: SessionState): string {
  const parts: string[] = [];
  for (const s of pendingSubmissions(state)) parts.push(`s:${s.id}`);
  const failed = lastUnavailable(state);
  for (const [i, r] of state.effectOrder.entries()) {
    if (i < failed || isFactStep(r.name)) continue;
    if (r.status === 'running' && r.park && r.settlement) parts.push(`p:${r.stepId}`);
    else if (r.status === 'pending') parts.push(`r:${r.stepId}`);
  }
  return parts.join(',');
}

/**
 * A companion stands down when the submission it guards is placed, nothing
 * is open, and a newer submission is waiting: that one has a wake of its own
 * (its direct drive may not have started yet, and must not be raced), and
 * whatever drives it also finishes the guarded turn if it was cut short.
 * With nothing newer waiting, the companion drives: a turn its drive left
 * unfinished between two effects resumes, and a finished one is idle at once.
 */
function companionStandsDown(state: SessionState, submission: string): boolean {
  if (!state.submissions.some((s) => s.id === submission) || !state.placed.has(submission)) return false;
  if (state.effectOrder.some((r) => r.status === 'running' && !isFactStep(r.name) && !(r.park && isDetached(r)))) return false;
  return pendingSubmissions(state).length > 0;
}

const secondsUntil = (at: number) => Math.max(1, Math.ceil((at - Date.now()) / 1000));

/**
 * The session is in a lane this driver does not serve. If there is work
 * there: a lane this driver can wake gets a fresh wake (keyed by this
 * delivery, so a queue's dedup window never swallows it: whoever moved the
 * session there may have died before its own wake went out); a lane driven
 * where nobody can wake it (`LaneOptions.remote`, a tab) with work and
 * nothing running is taken over, so a submission never waits for a tab that
 * may not exist. Returns whether this delivery should drive after all.
 */
async function elsewhere(
  options: DriverOptions,
  log: SessionLog,
  lane: Lane,
  anchor: Anchor,
  say: NonNullable<DriverOptions['log']>,
  /** This delivery just closed an escalation's sibling there: that lane's driver may have acked while it was open, so wake it. */
  closedSibling = false
): Promise<boolean> {
  const lanes = options.lanes!;
  const state = log.state;
  const runId = log.runId;
  const open = state.effectOrder.filter(
    (r) => r.status === 'running' && !isFactStep(r.name) && !(r.park && isDetached(r)) && (r.startedEpoch ?? 0) === state.epoch
  );
  if (!closedSibling && !workRemains(state) && open.length === 0) {
    say('skip: session is in another lane', { runId, lane });
    return false;
  }
  if (lanes.remote?.includes(lane)) {
    const to = lanes.serves[0];
    if (open.length > 0 || pendingSubmissions(state).length === 0 || !to) {
      say('skip: work in a lane driven elsewhere', { runId, lane, open: open.length });
      return false;
    }
    const handoff: HandoffOutput = { from: lane, to, reason: `takeover: work arrived while nothing drives the ${lane} lane` };
    const { won } = await commitHandoff(log, handoff, state.epoch, { anchor });
    if (!won) return false;
    void publishHint(options.world, runId, { eventType: 'handoff', slot: log.state.eventCount, to, epoch: log.state.epoch });
    new JsonLinesWriter(options.world, runId, liveStreamName(runId)).write({ type: 'lane', ...handoff, takeover: true });
    say('took over a remote lane with pending work', { runId, from: lane, to });
    return true;
  }
  // A tab wakes itself from the log; nothing here can.
  if (lane === 'browser') {
    say('skip: work in the browser lane, which wakes itself', { runId });
    return false;
  }
  try {
    await lanes.wake(runId, lane, { from: lane, to: lane, reason: 'redelivered: work in this lane' }, {
      idempotencyKey: `${runId}:${lane}:${state.epoch}:redeliver:${anchorKey(anchor)}`,
    });
    say('re-woke the lane the session is in', { runId, lane });
  } catch (err) {
    say('wake failed', { runId, to: lane, error: err instanceof Error ? err.message : String(err) });
    throw err;
  }
  return false;
}

/**
 * Close what this delivery was granted (intents anchored to its own message,
 * redelivered, or to the anchor a continuation handed it) that an earlier
 * epoch left open: the sibling of an escalation, which stays its drive's
 * until that drive closes, so the lane the session is in now finds it
 * covered. No fence: the epoch moved past it, so its old drive's next
 * intent is fenced already, and a late outcome of it races this resolution
 * first-wins.
 */
async function resolveGranted(
  world: World,
  policy: AnchorPolicy,
  log: SessionLog,
  anchor: Anchor,
  continues: string | undefined,
  say: NonNullable<DriverOptions['log']>
): Promise<number> {
  const vc: VerdictContext = { world, policy, own: anchor, ...(continues ? { continues } : {}), state: log.state };
  let resolved = 0;
  for (const record of [...log.state.effectOrder]) {
    if (record.status !== 'running' || isFactStep(record.name) || (record.park && isDetached(record))) continue;
    if ((record.startedEpoch ?? 0) >= log.state.epoch) continue;
    if ((await anchorVerdict(vc, record)).v !== 'mine') continue;
    if (record.park) await commitDetach(log, record, { drv: `drv_${ulid()}`, anchor }).catch(() => {});
    else await resolveAbandoned(world, log, record);
    resolved++;
    say('recovered a granted intent an earlier epoch left open', { runId: log.runId, key: record.key });
  }
  return resolved;
}

/** How a drive ended, for the exit path. */
type Exit =
  | { k: 'idle' }
  | { k: 'budget' }
  | { k: 'handoff'; to?: HandoffOutput }
  | { k: 'parked' }
  | { k: 'contended'; delaySeconds: number }
  | { k: 'preempted' }
  | { k: 'superseded' }
  | { k: 'done' }
  /** The harness cannot run here, and a queue message stands behind this direct drive: hand the turn to the queue. */
  | { k: 'queue'; info: UnavailableInfo };

/** Times a delivery decides again after the World refused its first in-band write (then it passes its grant on). */
const MAX_FENCE_REFUSALS = 3;

async function deliverOpen(
  driverOptions: DriverOptions,
  policy: AnchorPolicy,
  runId: string,
  meta: DeliverMeta,
  claim: ActiveDrive,
  /** How many times this delivery's first in-band write was refused already. */
  refusals = 0
): Promise<void | 'refused' | 'handed'> {
  let options = driverOptions;
  const { world } = options;
  const anchor = meta.anchor!;
  const log = meta.log?.runId === runId ? meta.log : new SessionLog(world, runId);
  if (log !== meta.log) await log.refresh();
  const say = options.log ?? (() => {});

  if (log.state.status !== 'running') {
    say('skip: session not running', { runId, status: log.state.status });
    return;
  }
  const harness = [options.harness]
    .flat()
    .find((h) => h.name === harnessOf(log.state.workflowName));
  if (!harness) {
    say('skip: no harness for session', { runId, workflowName: log.state.workflowName });
    return;
  }
  if (harness.lanes) options = { ...options, lanes: harness.lanes };
  // A continuation is spent once a recovery took it.
  const continues =
    meta.continues && !log.state.effectOrder.some((e) => e.name === 'recover' && (e.input as RecoverOutput | undefined)?.continues === meta.continues)
      ? meta.continues
      : undefined;
  let lane = options.lanes ? currentLane(log.state, options.lanes.initial) : undefined;
  if (options.lanes && !options.lanes.serves.includes(lane!)) {
    // Whatever this delivery was granted is its to close before it acks,
    // even in a lane it does not serve: the sibling an escalation left
    // running stays its drive's (anchorVerdict), so the next lane's driver
    // finds it covered, and nothing else comes back for it.
    const resolved = await resolveGranted(world, policy, log, anchor, continues, say);
    if (!(await elsewhere(options, log, lane!, anchor, say, resolved > 0))) return;
    lane = currentLane(log.state, options.lanes.initial);
  }
  const admission = (await options.admit?.(log.state)) ?? { admit: true };
  if (!admission.admit) {
    say('skip: not admitted', { runId, reason: admission.reason });
    // A later look is a new message: this one is acked. A grant it carried
    // goes with it (a redelivery hands its own message's work over).
    const grant = continues ?? ((meta.attempt ?? 1) > 1 ? anchorKey(anchor) : undefined);
    if (admission.timeoutSeconds || grant) await wake(world, runId, undefined, admission.timeoutSeconds ?? 1, grant ? { continues: grant } : {});
    return;
  }

  // 0. A companion whose turn is answered, with a newer one waiting, stands down.
  if (meta.companion && meta.guards && companionStandsDown(log.state, meta.guards)) {
    say("skip: the companion's turn is answered", { runId, submission: meta.guards });
    return;
  }

  // The in-band count this delivery decides from: the World's, for the view
  // the verdicts below read (session-log.ts `arm`). A World that fences but
  // lists no snapshot (world-vercel) is asked for it first, before deciding.
  const fenceOn = options.inBandFence !== false;
  if (fenceOn && log.needsProbe) await log.probe().catch(() => false);
  const decidedCount = log.inBandCount;

  // 1. Open work. Nothing is judged by time or heartbeat: each running
  //    intent names its anchor, and the verdict (anchor.ts) says whether it
  //    is this delivery's to recover (its own message redelivered, a
  //    continuation's), provably abandoned, or held by a drive that takes it
  //    if it lives and whose message comes back if it died.
  const drv = `drv_${ulid()}`;
  claim.drv = drv;
  const vc: VerdictContext = { world, policy, own: anchor, ...(continues ? { continues } : {}), state: log.state };
  const abandoned: EffectRecord[] = [];
  const toDetach: EffectRecord[] = [];
  const held: Array<{ record: EffectRecord; why: string }> = [];
  let fence = false;
  for (const record of [...log.state.effectOrder]) {
    if (record.status !== 'running') continue;
    if (isFactStep(record.name)) {
      // Half-written: a handoff, park, settlement or crash took effect at
      // its intent, so finish it.
      await finishFact(log, record);
      continue;
    }
    // Parked and let go of: its owner (a sandbox command) has it, and owner
    // checks police it. Still attached, it is its parker's open work.
    if (record.park && isDetached(record)) continue;
    const verdict = await anchorVerdict(vc, record);
    if (verdict.v === 'covered') {
      held.push({ record, why: verdict.why });
      continue;
    }
    if (verdict.fence && (record.startedEpoch ?? 0) === log.state.epoch) fence = true;
    (record.park ? toDetach : abandoned).push(record);
  }
  const marking = marksEnabled(options);
  const live = new JsonLinesWriter(world, runId, liveStreamName(runId));
  const runAhead = runAheadFor(options, log.state);
  // Run-ahead: one pipeline per log, shared with a submit that just wrote to
  // it (the drive route), so the drive starts on the speculative fold.
  const pipe = runAhead ? PipelinedLog.for(log, { log: say }) : undefined;
  const note = (name: MarkName, fields: Partial<Omit<Mark, 'type' | 'name' | 'runId'>> = {}) => {
    if (marking) writeMark(live, log, name, { ...(lane ? { lane } : {}), ...fields });
  };
  if (held.length > 0) {
    // Resolve what is provably abandoned first (first-wins, idempotent):
    // acking with it unresolved would strand it, since its own message may
    // be this very one. No fence: the epoch belongs to the live holder.
    if (abandoned.length > 0 || toDetach.length > 0) {
      if (pipe) {
        await pipe.settled();
        pipe.reset();
      }
      for (const record of abandoned) await resolveAbandoned(world, log, record);
      for (const record of toDetach) await commitDetach(log, record, { drv, anchor }).catch(() => {});
    }
    // A companion never lets go of a direct drive before its deadline: it
    // could still die, with nothing else behind it.
    if (meta.companion) {
      const at = Math.max(0, ...held.map((h) => (h.record.anchor?.k === 'deadline' ? (h.record.anchor.at ?? 0) : 0)));
      if (at > Date.now()) {
        await wake(world, runId, `${runId}:companion:${anchorKey(anchor)}:${at}`, secondsUntil(at) + 5, {
          companion: true,
          ...(meta.guards ? { guards: meta.guards } : {}),
        });
        say('defer: a direct drive is within its deadline', { runId, at });
        return;
      }
    }
    say('skip: open work is held elsewhere', { runId, keys: held.map((h) => h.record.key), why: held[0]!.why });
    await live.flush();
    return;
  }

  // 2. Recover. A redelivery of a message, or a continuation, fences
  //    whatever the fold shows: its predecessor's last writes may not be
  //    durable yet (running ahead, frozen), and must come back fenced.
  if ((meta.attempt ?? 1) > 1 || continues) fence = true;
  // A write whose reply never came but which the log shows committed (see
  // SessionLog.slowWriteProbeMs): worth a line, it is a transport fault.
  log.onSlowWrite ??= (w) => say('slow write found committed', { runId, ...w });
  log.onFence = (n) => {
    if (n.kind === 'refused') say('in-band fence: a write was refused', { runId, drv, expected: n.expected, ...(n.seqInBand !== undefined ? { seqInBand: n.seqInBand } : {}) });
    else if (n.kind === 'legacy') say('in-band fence: unfenced', { runId, drv, reason: n.reason });
  };
  if (marking) {
    log.onWrite = (w) =>
      note('ack', {
        eventType: w.eventType,
        ...(w.slot !== null ? { slot: w.slot } : {}),
        lagMs: round(w.ms),
        ...(w.bumped ? { bumped: true } : {}),
        // Run-ahead: how far behind the harness this write became durable.
        ...(pipe ? { aheadMs: pipe.lag.oldestMs, pending: pipe.lag.pending } : {}),
      });
  }
  const admittedEpoch = log.state.epoch;
  let fenceEpoch = admittedEpoch;
  const recovering = abandoned.length > 0 || toDetach.length > 0 || fence;
  // From here this delivery drives: its writes are in-band, at the count of
  // the view it decided from (`decidedCount`, session-log.ts `arm`).
  // Everything it wrote before (finishing half-written facts) was
  // out-of-band; if another drive wrote in-band meanwhile, the World refuses
  // this one's first write and it decides again.
  const fencing = fenceOn && log.arm(drv, decidedCount);
  if (fencing) say('in-band fence: armed', { runId, drv });
  /** The World refused this delivery's recovery: someone else wrote in-band since its load. */
  const recoveryRefused = async (): Promise<void | 'refused'> => {
    log.disarm(drv);
    if (log.state.epoch !== admittedEpoch) {
      say('skip: another driver took the session first', { runId });
      return;
    }
    if (refusals + 1 < MAX_FENCE_REFUSALS) return 'refused';
    // Still losing to a live writer: pass the grant on (I5), or look again later.
    const grant = continues ?? ((meta.attempt ?? 1) > 1 ? anchorKey(anchor) : undefined);
    say('in-band fence: refused again; passing the delivery on', { runId, ...(grant ? { grant } : {}) });
    await wake(world, runId, `${runId}:refused:${anchorKey(anchor)}:${drv}`, options.contendedDelaySeconds ?? 2, grant ? { continues: grant } : {});
  };
  // Recovery decides who acts: nothing of ours may still be in flight.
  if (recovering && pipe) {
    await pipe.settled();
    pipe.reset();
  }
  if (recovering) {
    // Re-announce the crashes that prove these intents dead. The dying
    // process writes its own `chaos` live event, but a SIGKILL can take the
    // write with it (and kills any connection the same instance was serving),
    // so a reader waiting on the live stream would never hear of the crash.
    // The log has it; say it again from here. Readers tolerate a repeat.
    const announced = new Set<string>();
    for (const record of abandoned) {
      const proof = log.state.effectOrder.find((e) => {
        const out = chaosOutputOf(e);
        if (!out || announced.has(e.stepId)) return false;
        return out.v === 2 ? !!out.driver && out.driver === record.driver : out.crashedDuring === record.key;
      });
      if (!proof) continue;
      announced.add(proof.stepId);
      live.write({ type: 'chaos', ...chaosOutputOf(proof)!, reannounced: true, t: markTime(), pid: PROCESS_ID });
    }
    note('recover.start', { keys: abandoned.map((r) => r.key), proven: fence ? 'anchor' : 'log' });
  }
  if (fence) {
    let won: boolean;
    try {
      ({ won } = await commitEpochStep(
        log,
        'recover',
        {
          ...(lane ? { lane } : {}),
          reason: continues ? 'recover: handed over by a continuation' : abandoned.length ? 'recover: the holder of these intents is gone' : 'recover: a redelivery',
          keys: abandoned.map((r) => r.key),
          ...(continues ? { continues } : {}),
        },
        admittedEpoch,
        { drv, anchor }
      ));
    } catch (err) {
      if (err instanceof LogSuperseded) return recoveryRefused();
      log.disarm(drv);
      throw err;
    }
    if (!won) {
      log.disarm(drv);
      say('skip: another driver took the session first', { runId });
      return;
    }
    fenceEpoch = admittedEpoch + 1;
    // Whoever drove before stops at once (the hint), not at its next write.
    void publishHint(world, runId, { eventType: 'recover', slot: log.state.eventCount, epoch: fenceEpoch });
  }
  for (const record of abandoned) {
    if (log.superseded) break;
    await resolveAbandoned(world, log, record);
    say('recovered abandoned intent', { runId, key: record.key, replay: record.replay });
  }
  for (const record of toDetach) {
    if (log.superseded) break;
    await commitDetach(log, record, { drv, anchor }).catch(() => {});
    say('detached a parked step its parker left', { runId, key: record.key });
  }
  // A resolution the World refused (they are written without a fence step
  // when the log alone proves the holder gone): nothing of it landed.
  if (log.superseded) return recoveryRefused();
  if (recovering) note('recover.done', { keys: abandoned.map((r) => r.key) });
  // Recovery writes fold in whatever landed meanwhile. If another driver
  // moved the session (a takeover handoff, a recovery of its own), this
  // delivery must not adopt its epoch: it would drive as if it were the new
  // owner.
  if (
    log.state.epoch !== fenceEpoch ||
    (options.lanes && currentLane(log.state, options.lanes.initial) !== lane)
  ) {
    log.disarm(drv);
    say('skip: the session moved during recovery', { runId, epoch: log.state.epoch });
    return;
  }

  // 3. Drive.
  const epoch = log.state.epoch;
  /** The crash armed when this drive began (`ChaosDriveOptions.fire: 'armed-in-drive'` leaves it armed). */
  const armedAtStart = armedChaos(log.state)?.id;
  const hard = await hardDeadline(world, options, anchor);
  const margin = options.deadlineMarginMs ?? 15_000;
  const deadline = hard - margin;
  /** Past this, the exit path stops waiting for work in flight (a reserve for the wake). */
  const until = () => Math.max(hard - Math.min(1000, margin / 2), Date.now() + BUDGET_FINISH_FLOOR_MS);
  const releaseHold = await policy.hold?.(runId);
  const hub = new HintHub(world, runId);
  /** Set when this delivery gave its message up early (the watchdog): nothing of the drive may send anything after. */
  let released = false;
  /** Set once this direct drive handed its turn to the queue (`Exit` `queue`). */
  let handedOver = false;
  /**
   * A direct drive with a queue message behind it (`DeliverMeta.backstopped`):
   * a harness that cannot run here is the queue's to try, not a failed turn.
   */
  const handsOver = meta.backstopped === true && anchor.k === 'deadline';
  /**
   * Whether a write of this drive failed in a way that leaves its outcome
   * unknown (it may land later): the drive then closes (a `close` fact)
   * before it lets go, so an intent of it landing after that is abandoned
   * for every reader rather than held by a delivery that already acked.
   */
  const unsure = { value: false };
  let current: Drive | undefined;
  const offEpoch = hub.on((h) => {
    if (h.eventType !== 'recover' && h.eventType !== 'handoff' && h.eventType !== 'reconnect') return;
    if (h.epoch !== undefined && h.epoch <= epoch) return;
    if (pipe && pipe.lag.pending > 0) {
      // A refresh would queue behind the stalled write (the log's lock)
      // while the drive runs on: fence the pipeline now, outside the lock.
      if (h.epoch !== undefined) pipe.fence(`the session moved to epoch ${h.epoch}`);
      return;
    }
    void log.refresh().catch(() => {});
  });
  // Fallback for a lost hint: a read, so a preempted drive stops in time.
  const refresher = setInterval(() => {
    if (pipe && pipe.lag.pending > 0) return;
    void log.refresh().catch(() => {});
  }, PREEMPTION_REFRESH_MS);
  (refresher as { unref?: () => void }).unref?.();
  note('drive.start', { drv, epoch, attempt: meta.attempt ?? 0, anchor: anchor.k, ...(meta.companion ? { companion: true } : {}), ...processInfo('drive') });
  // The first drive in the lane a handoff named: the receiving half of that handoff.
  const lastEpochStep = [...log.state.effectOrder].reverse().find((r) => isEpochStep(r.name) && r.status === 'completed');
  const handedIn = lastEpochStep?.name === 'handoff' ? (lastEpochStep.output as HandoffOutput | undefined) : undefined;
  if (lane && handedIn?.to === lane && !log.state.effectOrder.some((r) => r.startedEpoch === epoch && !isEpochStep(r.name))) {
    note('handoff.picked', { drv, from: handedIn.from, to: handedIn.to, reason: handedIn.reason, epoch });
  }

  /** Send the session on to a continuation: this delivery's open work becomes its. */
  const continuation = async (why: string, delaySeconds = 1) => {
    say('continuation', { runId, why });
    try {
      await wake(world, runId, `${runId}:cont:${anchorKey(anchor)}:${meta.attempt ?? 1}:${drv}`, delaySeconds, {
        continues: anchorKey(anchor),
      });
    } catch (err) {
      // Nothing can carry the work on: give the message back instead, and
      // the queue's redelivery of it recovers (and fences) this drive.
      throw new ReleaseAnchor(`${why}; the continuation could not be sent (${String(err)})`);
    }
  };

  /** Wait for the effects the harness left running; false past `until`. */
  const settleEffects = async (drive: Drive): Promise<boolean> => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const finished = await Promise.race([
      Promise.allSettled(drive.inFlight()).then(() => true as const),
      new Promise<false>((r) => (timer = setTimeout(() => r(false), until() - Date.now()))),
    ]);
    clearTimeout(timer);
    return finished;
  };

  /** Wait for the effects the harness left running, then this drive's writes; false past `until`. */
  const finishInFlight = async (drive: Drive): Promise<boolean> => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const finished = await Promise.race([
      (async () => {
        await Promise.allSettled(drive.inFlight());
        if (pipe) {
          await pipe.settled();
          pipe.reset();
          await reissueOrphans(pipe);
          await pipe.settled();
        }
        return true;
      })(),
      new Promise<false>((r) => (timer = setTimeout(() => r(false), until() - Date.now()))),
    ]);
    clearTimeout(timer);
    return finished;
  };

  /**
   * What this drive leaves open, closed before it lets go: an intent of it
   * still running (its write was ambiguous and landed after all) is released
   * as a recovery would; a parked step it is still attached to is detached.
   * Returns whether a step it parked settled meanwhile (a detach that lost):
   * the drive takes that settlement itself instead of letting go.
   */
  const releaseOwn = async (): Promise<boolean> => {
    await log.refresh().catch(() => {});
    let settled = false;
    for (const r of [...log.state.effectOrder]) {
      if (r.status !== 'running' || r.driver !== drv || isFactStep(r.name)) continue;
      if (r.park) {
        if (r.settlement) settled = true;
        else if (!r.detached) {
          const { won } = await commitDetach(log, r, { drv, anchor }).catch(() => ({ won: false }));
          if (!won && log.state.effects.get(r.key)?.settlement) settled = true;
        }
        continue;
      }
      await resolveAbandoned(world, log, r);
    }
    return settled;
  };

  const close = async () => {
    if (!unsure.value) return;
    await commitClose(log, drv, { anchor }).catch((err) => say('close failed', { runId, drv, error: String(err) }));
  };

  /**
   * Fail the turn (unavailable.ts): the `unavailable` fact, then the live
   * `error` and `idle`. False when the epoch moved (another drive owns the
   * session now, and decides for itself).
   */
  const failTurn = async (unavailable: UnavailableInfo): Promise<boolean> => {
    const hint = options.unavailableHint?.(unavailable, harness.name) ?? unavailable.hint ?? defaultHint(unavailable, harness.name);
    const failed = await commitUnavailable(
      log,
      live,
      { ...unavailable, ...(hint ? { hint } : {}), harness: harness.name, ...(meta.attempt !== undefined ? { attempt: meta.attempt } : {}) },
      { epoch, drv, anchor }
    );
    if (failed) say('turn failed: the harness cannot run here', { runId, code: unavailable.code, error: unavailable.message });
    return failed;
  };

  /** Stop at once without writing anything more, and hand the session to a continuation. */
  const giveUp = async (why: string) => {
    released = true;
    log.fence(`released: ${why}`);
    pipe?.fence(`released: ${why}`);
    current?.halt(new Yield('preempted'));
    await continuation(why);
  };

  const runDrive = async (): Promise<Exit> => {
    let restarts = 0;
    let lastRestart = '';
    let lastWork = '';
    for (let pass = 0; ; pass++) {
      if (log.state.epoch !== epoch) {
        say('yield: preempted', { runId });
        return { k: 'preempted' };
      }
      if (log.superseded) return { k: 'superseded' };
      const budget = new AbortController();
      const timer = setTimeout(() => budget.abort(new Yield('budget')), Math.max(0, deadline - Date.now()));
      if (pipe?.failed) {
        // A write issued before this attempt (a submit's) was rejected.
        await pipe.settled();
        pipe.reset();
        await log.refresh();
      }
      if (pipe && (await reissueOrphans(pipe))) await log.refresh();
      const drive = createContext(options, log, live, budget.signal, restarts, { drv, anchor, unsure, armedAtStart, deadline }, marking, hintsEnabled(options) ? hub : undefined, pipe, log === meta.log && pass === 0);
      current = drive;
      let exit: Exit | undefined;
      try {
        await tracedDrive(harness, drive.ctx);
        // A rejection that stopped the drive while the harness wound down.
        const halted = drive.halted();
        if (halted) throw halted;
        if (drive.preempted()) return { k: 'preempted' };
        // Nothing of this drive may be left running when it lets go: effects
        // the harness did not wait for, then its writes (durable before
        // deciding the session is idle, or giving it up).
        if (!(await settleEffects(drive))) {
          drive.dispose();
          return { k: 'budget' };
        }
        if (pipe) {
          try {
            await pipe.drain();
          } catch (err) {
            throw drive.halted() ?? new Restart(`run-ahead: ${(err as Error).message}`);
          }
        }
        drive.dispose();
        if (drive.preempted()) return { k: 'preempted' };
        // Anything that arrived while the harness was finishing?
        if (await releaseOwn()) continue;
        const work = workFingerprint(log.state);
        if (work !== '' && work !== lastWork && pass < MAX_PASSES) {
          lastWork = work;
          continue;
        }
        if (work !== '') say('idle with work the harness did not take', { runId, work });
        exit = { k: 'idle' };
      } catch (thrown) {
        drive.dispose();
        if (drive.preempted()) return { k: 'preempted' };
        // A drive stopped from outside the harness's own flow (`ctx.halt`, a
        // run-ahead rejection): that reason is the outcome, whatever the
        // harness threw on its way out (an abort error from its child).
        const halted = drive.halted();
        const err = halted && !(thrown instanceof Fatal) ? halted : thrown;
        if (err instanceof Restart) {
          say(err.message, { runId });
          lastRestart = err.message;
          if (++restarts >= MAX_RESTARTS) {
            // The same log, rebuilt MAX_RESTARTS times, never got past the
            // same spot: redelivering would only repeat it.
            const message = `harness-world: ${runId} restarted too many times (${lastRestart})`;
            await failSession(world, log, live, message);
            say('session failed', { runId, error: message });
            return { k: 'done' };
          }
          if (!(await finishInFlight(drive))) return { k: 'budget' };
          await log.refresh();
          continue;
        }
        if (err instanceof Fatal) {
          await failSession(world, log, live, err.message);
          say('session failed', { runId, error: err.message });
          return { k: 'done' };
        }
        if (err instanceof Yield || budget.signal.aborted) {
          const y = err instanceof Yield ? err : (budget.signal.reason as Yield);
          say(y.message, { runId });
          if (y.reason === 'preempted') return { k: 'preempted' };
          if (y.reason === 'superseded') return { k: 'superseded' };
          if (y.reason === 'budget') return { k: 'budget' };
          // Siblings of the effect that yielded may still be running: this
          // drive is theirs until they land, then it lets go.
          if (!(await finishInFlight(drive))) return { k: 'budget' };
          if (y.reason === 'parked') {
            if (await releaseOwn()) continue;
            return { k: 'parked' };
          }
          if (y.reason === 'handoff') return { k: 'handoff', ...(drive.handoff() ? { to: drive.handoff() } : {}) };
          return { k: 'contended', delaySeconds: Math.max(1, y.delaySeconds) };
        }
        // The harness cannot run here (unavailable.ts): fail the turn, keep the session.
        const unavailable = isKernelError(err) || log.superseded ? undefined : (unavailableOf(err) ?? failingOf(err, meta.attempt, options.maxFailedDeliveries ?? MAX_FAILED_DELIVERIES));
        if (unavailable) {
          if (!(await finishInFlight(drive))) return { k: 'budget' };
          // What this drive left running goes back the way a recovery would release it.
          await releaseOwn();
          await log.refresh();
          if (log.state.epoch !== epoch) return { k: 'preempted' };
          // A direct drive with a queue message behind it: the Function that
          // serves the queue is another bundle, and may have what this one
          // lacks. It gets the turn now, not past this drive's deadline.
          if (handsOver && unavailable.code !== 'failing') return { k: 'queue', info: unavailable };
          return (await failTurn(unavailable)) ? { k: 'idle' } : { k: 'preempted' };
        }
        throw err;
      } finally {
        clearTimeout(timer);
      }
      return exit;
    }
  };

  /**
   * The World's in-band fence refused a write of this drive: another drive
   * wrote in-band since this one loaded, at the same epoch (a takeover that
   * moved the epoch is `preempted`: the epoch closes every intent of this
   * drive). Nothing more of this drive goes to the log, not even a release or
   * a `close`: they would be refused too, and a refusal means the log is no
   * longer the one this drive decided from. Open work of its own (I2) goes to
   * a continuation, whose delivery recovers it at a fresh count (I4); a grant
   * it still holds is passed on (I5); with neither, a look later, as for
   * `contended`.
   */
  const leaveSuperseded = async (): Promise<void> => {
    if (released) return;
    released = true;
    const drive = current;
    if (drive) {
      // Its effects have the abort; give them a moment to stop, so a replay-safe one is not run twice at once.
      let timer: ReturnType<typeof setTimeout> | undefined;
      await Promise.race([
        Promise.allSettled(drive.inFlight()),
        new Promise((r) => (timer = setTimeout(r, Math.max(0, Math.min(SUPERSEDED_SETTLE_MS, until() - Date.now()))))),
      ]);
      clearTimeout(timer);
    }
    await log.refresh().catch(() => {});
    if (log.state.epoch !== epoch) {
      say('yield: preempted', { runId });
      return;
    }
    const own = log.state.effectOrder.filter(
      (r) => r.status === 'running' && r.driver === drv && !isFactStep(r.name) && !(r.park && isDetached(r))
    );
    say('yield: superseded', { runId, drv, open: own.length });
    note('release', { drv, reason: 'superseded' });
    if (own.length > 0) {
      await continuation('superseded: another drive wrote in-band; open work goes to a continuation');
      return;
    }
    const grant = continues ?? ((meta.attempt ?? 1) > 1 ? anchorKey(anchor) : undefined);
    await wake(world, runId, `${runId}:superseded:${drv}`, grant ? 1 : (options.contendedDelaySeconds ?? 2), grant ? { continues: grant } : {});
  };

  /** What the drive does once it stopped: close, hand over, wake. */
  const exitWith = async (exit: Exit): Promise<void> => {
    if (exit.k === 'superseded') return leaveSuperseded();
    try {
      await exitNormally(exit);
    } catch (err) {
      if (!(err instanceof LogSuperseded)) throw err;
    }
    // A write on the way out (a release, a handback, a close) was refused.
    if (log.superseded) await leaveSuperseded();
  };

  const exitNormally = async (exit: Exit): Promise<void> => {
    switch (exit.k) {
      case 'idle': {
        await settleLane(options, log, live, epoch, { drv, anchor }, marking ? (h) => note('handoff.sent', { drv, ...h }) : undefined);
        note('idle', { drv });
        await close();
        return;
      }
      case 'budget': {
        const drive = current!;
        if (await finishInFlight(drive)) {
          // Clean: nothing of this drive is open. The next delivery goes on at once.
          await releaseOwn();
          await close();
          if (!released) await wake(world, runId, `${runId}:budget:${log.state.eventCount}:${drv}`);
          return;
        }
        if (released) return;
        // Work still in flight past the reserve: write nothing more from here
        // (a dangling `never` effect may still complete: "may or may not have
        // taken effect"), and hand the session over.
        say('budget: work still in flight', { runId, effects: drive.inFlight().length, ...(pipe ? { pending: pipe.lag.pending } : {}) });
        released = true;
        log.fence('yielded');
        pipe?.fence('yielded');
        await continuation('budget: work still in flight');
        return;
      }
      case 'handoff': {
        await releaseOwn();
        await close();
        // A sibling still running made the receiving lane's first delivery
        // find it open and ack: wake that lane again now that it landed.
        const lanes = options.lanes;
        if (exit.to && lanes && current?.siblingsAtHandoff() && !released) {
          await lanes
            .wake(runId, exit.to.to, exit.to, { idempotencyKey: `${runId}:${exit.to.to}:${log.state.epoch}:settled:${drv}` })
            .catch((err) => say('wake failed', { runId, to: exit.to!.to, error: String(err) }));
        }
        return;
      }
      case 'parked':
        await close();
        return;
      case 'contended':
        await releaseOwn();
        await close();
        if (!released) await wake(world, runId, `${runId}:contended:${drv}`, exit.delaySeconds);
        return;
      case 'queue': {
        // What this drive left running is released already (the catch path).
        await close();
        try {
          // A continuation, at once: its delivery fences this drive's epoch and
          // drives the turn; one that meets the same failure fails the turn.
          // In this process it waits for this drive to let go (a grant).
          await wake(world, runId, `${runId}:unavailable:${anchorKey(anchor)}:${drv}`, 0, { continues: anchorKey(anchor) });
        } catch (err) {
          say('the queue could not take the turn: failing it here', { runId, error: String(err) });
          if (await failTurn(exit.info)) await exitNormally({ k: 'idle' });
          return;
        }
        released = true;
        handedOver = true;
        log.fence('handed to the queue');
        pipe?.fence('handed to the queue');
        say('handed to the queue: the harness cannot run here', { runId, code: exit.info.code, error: exit.info.message });
        note('release', { drv, reason: `unavailable here (${exit.info.code}): handed to the queue` });
        return;
      }
      case 'preempted':
      case 'superseded':
      case 'done':
        return;
    }
  };

  let watchdog: ReturnType<typeof setInterval> | undefined;
  // Past the budget and the time the exit path may spend finishing up.
  const tripAt = Math.max(hard - 1000, deadline + BUDGET_FINISH_FLOOR_MS + 1000);
  try {
    // The drive races a local watchdog that writes nothing: a drive whose
    // durability stalled, whose write hangs, or that is still going at its
    // hard deadline gives its delivery up without waiting for whatever it is
    // stuck on, so its session is never held by a delivery that cannot move.
    const tripped = new Promise<string>((resolve) => {
      watchdog = setInterval(() => {
        if (pipe && pipe.lag.oldestMs > RUN_AHEAD_DEMOTE_MS && pipe.liveHeld === 0) resolve('demoted: durability stalled');
        else if (log.stalledMs > (options.writeStallMs ?? WRITE_STALL_MS) && (!pipe || pipe.liveHeld === 0)) resolve('a write stalled');
        else if (Date.now() >= tripAt) resolve('past the hard deadline');
      }, 250);
      (watchdog as { unref?: () => void }).unref?.();
    });
    const driven = runDrive()
      .then(exitWith)
      .then(
        () => ({ ok: true as const }),
        (error: unknown) => ({ error })
      );
    const first = await Promise.race([driven, tripped.then((why) => ({ why }))]);
    if ('why' in first) {
      say('releasing the delivery', { runId, drv, why: first.why });
      note('release', { drv, reason: first.why });
      await giveUp(first.why);
      return;
    }
    if ('error' in first) throw first.error;
  } finally {
    clearInterval(watchdog);
    clearInterval(refresher);
    offEpoch();
    hub.close();
    log.disarm(drv);
    if (!released && pipe) {
      // Drain before letting go.
      await pipe.settled();
      pipe.reset();
      await reissueOrphans(pipe);
    }
    log.onWrite = undefined;
    releaseHold?.();
    await live.flush();
  }
  if (handedOver) return 'handed';
}

const round = (ms: number) => Math.round(ms * 10) / 10;

export function hintsEnabled(options: Pick<DriverOptions, 'hints'>): boolean {
  return options.hints ?? process.env.HARNESS_HINTS !== '0';
}

/**
 * One delivery's hint subscription (hints.ts), shared by the driver (epoch
 * hints) and every drive attempt's `ctx.onHint`.
 */
class HintHub {
  private readonly listeners = new Set<(hint: Hint) => void>();
  private abort?: AbortController;
  constructor(
    private readonly world: World,
    private readonly runId: string
  ) {}
  on(listener: (hint: Hint) => void): () => void {
    this.listeners.add(listener);
    if (!this.abort) {
      this.abort = new AbortController();
      watchHints(this.world, this.runId, (h) => {
        for (const l of this.listeners) l(h);
      }, { signal: this.abort.signal });
    }
    return () => this.listeners.delete(listener);
  }
  close(): void {
    this.abort?.abort();
    this.listeners.clear();
  }
}

const MAX_RESTARTS = 8;

/** The kernel's own control flow, never a harness failure (unavailable.ts does not judge these). */
const isKernelError = (err: unknown) =>
  err instanceof LogSuperseded || err instanceof LogFenced || err instanceof ReleaseAnchor || err instanceof Rejection || err instanceof Parked;
/** "More work arrived" passes of one delivery before it acks (each pass must see new work). */
const MAX_PASSES = 32;

/**
 * One effect's crash decision (see `armChaos`): `open` while it runs,
 * `parking` while it records a park, `settling` once its work returned (the
 * timer backs off), `crashing` once the timer decided (the effect never
 * records a result).
 */
interface ChaosGate {
  state: 'open' | 'parking' | 'settling' | 'crashing';
}

/** Wait for a kill that has been decided: nothing after this runs. */
const forever = () => new Promise<never>(() => {});

/** Fail the run: the terminal outcome for a session that cannot continue. Its streams end with it. */
async function failSession(world: World, log: SessionLog, live: JsonLinesWriter, message: string) {
  await log.refresh();
  if (log.state.status !== 'running') return;
  try {
    await log.write({ eventType: 'run_failed', eventData: { error: encode({ message }) } });
  } catch (err) {
    // Refused by the in-band fence: the session is another drive's, which meets the same failure.
    if (err instanceof LogSuperseded) return;
    throw err;
  }
  live.write({ type: 'failed', message });
  await live.flush();
  await closeRunStreams(world, log.runId, kernelStreamNames(log.runId));
}

/** Idle in a lane other than the settle lane: hand the session back. */
async function settleLane(
  options: DriverOptions,
  log: SessionLog,
  live: JsonLinesWriter,
  epoch: number,
  by: { drv: string; anchor: Anchor },
  sent?: (handoff: HandoffOutput) => void
) {
  const lanes = options.lanes;
  if (!lanes) return;
  const home = homeLane(log.state, lanes);
  if (!home) return;
  // Someone else moved the session while this drive was finishing.
  if (log.state.epoch !== epoch) return;
  const from = currentLane(log.state, lanes.initial);
  if (from === home || !lanes.serves.includes(from)) return;
  // A home lane that cannot take the session now (a sandbox with no live host) keeps it here.
  if (lanes.available && !lanes.available(home, log.state)) return;
  const handoff: HandoffOutput = { from, to: home, reason: 'idle' };
  const { won } = await commitHandoff(log, handoff, epoch, by);
  // Lost the race: the other writer owns the session and wakes its lane.
  if (!won) return;
  // The session is the home lane's now: whatever this drive writes after this is not in-band.
  if (by.drv) log.disarm(by.drv);
  live.write({ type: 'lane', ...handoff });
  void publishHint(options.world, log.runId, { eventType: 'handoff', slot: log.state.eventCount, to: handoff.to, epoch: log.state.epoch });
  sent?.(handoff);
  await wakeLane(options, log.runId, lanes, handoff);
  // Work that landed between the last read and the handback, in a lane
  // nobody here can wake (a tab that may be gone): wake a delivery here,
  // which takes it back if nothing drives it there.
  if (lanes.remote?.includes(home)) {
    await log.refresh().catch(() => {});
    if (pendingSubmissions(log.state).length > 0 && currentLane(log.state, lanes.initial) === home) {
      await wake(options.world, log.runId, `${log.runId}:handback:${log.state.eventCount}`).catch(() => {});
    }
  }
}

/**
 * Wake the lane a handoff names. A failure is logged, not thrown: the
 * handoff is committed, so the session is already in that lane, and a
 * delivery that finds it there with work wakes it again (`elsewhere`).
 * Throwing would hand a harness a plain error in the middle of a tool call
 * (fx reports those to the model as the tool's result).
 */
export async function wakeLane(options: Pick<DriverOptions, 'log'>, runId: string, lanes: LaneOptions, handoff: HandoffOutput) {
  try {
    await lanes.wake(runId, handoff.to, handoff);
  } catch (err) {
    options.log?.('wake failed', { runId, to: handoff.to, error: err instanceof Error ? err.message : String(err) });
  }
}

/**
 * Commit a handoff as the writer of `epoch`: one born-running `handoff:<epoch>`
 * step (first writer wins, and fenced if the epoch moved), then its outcome.
 * `won: false` means someone else moved the session first, so the caller
 * must not announce, wake, or drive anything. `by` names the drive (and its
 * anchor) moving it: an escalation keeps that drive's sibling effects its own
 * until it closes.
 */
export function commitHandoff(
  log: SessionLog,
  handoff: HandoffOutput,
  epoch: number,
  by: { drv?: string; anchor?: Anchor } = {}
): Promise<{ won: boolean }> {
  return commitEpochStep(log, 'handoff', handoff, epoch, by);
}

/** A `recover` step's input: the lane it recovers in, and what it took over. */
export interface RecoverOutput {
  lane?: Lane;
  reason: string;
  keys: string[];
  /** The anchor a continuation handed over (spent once recovered). */
  continues?: string;
}

/** Commit a `handoff` or a `recover` (see `isEpochStep`): both move the epoch. */
export async function commitEpochStep(
  log: SessionLog,
  name: 'handoff' | 'recover',
  input: HandoffOutput | RecoverOutput,
  epoch: number,
  by: { drv?: string; anchor?: Anchor } = {}
): Promise<{ won: boolean }> {
  const { won } = await commitFact(log, name, `handoff:${epoch}`, input, {
    epoch,
    ...(by.drv ? { drv: by.drv } : {}),
    ...(by.anchor ? { anchor: by.anchor } : {}),
  });
  return { won };
}

/** What a session wake carries besides its run id. */
export interface WakeOptions {
  /** A companion standing behind a direct drive (see `DeliverMeta.companion`). */
  companion?: boolean;
  /** The submission a companion stands behind. */
  guards?: string;
  /** A continuation: the anchor (`anchorKey`) whose open work it takes over. */
  continues?: string;
}

/** Enqueue a delivery for a session. */
export async function wake(
  world: World,
  runId: string,
  idempotencyKey?: string,
  delaySeconds?: number,
  options: boolean | WakeOptions = {}
): Promise<void> {
  const o: WakeOptions = typeof options === 'boolean' ? { companion: options } : options;
  // A companion's key is `<runId>:<submission id>`: the submission it guards.
  const guards = o.guards ?? (o.companion && idempotencyKey?.startsWith(`${runId}:`) ? idempotencyKey.slice(runId.length + 1) : undefined);
  await world.queue(
    QUEUE_NAME,
    {
      runId,
      input: {
        ...(o.companion ? { companion: true } : {}),
        ...(o.companion && guards ? { guards } : {}),
        ...(o.continues ? { continues: o.continues } : {}),
      },
    },
    {
      ...(idempotencyKey ? { idempotencyKey } : {}),
      ...(delaySeconds ? { delaySeconds: Math.ceil(delaySeconds) } : {}),
    }
  );
}

/**
 * When this delivery must have stopped writing: the platform's deadline for
 * the invocation if it reports one, at most `maxInvocationMs` away, and never
 * past the anchor's own deadline (a direct drive's).
 */
async function hardDeadline(world: World, options: DriverOptions, anchor: Anchor): Promise<number> {
  const reported = await world.getRuntimeDeadline?.().catch(() => undefined);
  const cap = Date.now() + (options.maxInvocationMs ?? 10 * 60_000);
  let hard = reported ? Math.min(reported.getTime(), cap) : cap;
  if (anchor.at !== undefined) hard = Math.min(hard, anchor.at);
  return hard;
}

/** The deadline a direct drive starting now would declare (see `DriverOptions.directDriveMs`). */
export async function directDeadline(world: World, options: Pick<DriverOptions, 'directDriveMs' | 'maxInvocationMs'>): Promise<number> {
  const reported = await world.getRuntimeDeadline?.().catch(() => undefined);
  const cap = Date.now() + Math.min(options.directDriveMs ?? DEFAULT_DIRECT_DRIVE_MS, options.maxInvocationMs ?? 10 * 60_000);
  return reported ? Math.min(reported.getTime(), cap) : cap;
}

/**
 * Resolve an intent whose driver is proven gone: a `safe` effect is
 * released for a retry, a `never` one gets an interrupted result with its
 * partial output, a half-written handoff is finished. If the driver's own
 * outcome landed first (it was only slow), that stands: the fold ignores an
 * outcome for a record that is no longer running, and a World that refuses
 * it outright is fine too.
 */
export async function resolveAbandoned(
  world: World,
  log: SessionLog,
  record: EffectRecord
): Promise<void> {
  if (isFactStep(record.name)) return finishFact(log, record);
  const partial = await readJsonLines(
    world,
    log.runId,
    effectStreamName(log.runId, record.stepId)
  );
  const request: CreateEventRequest =
    record.replay === 'safe'
      ? {
          eventType: 'step_retrying',
          correlationId: record.stepId,
          eventData: {
            stepName: stepNameFor(record.name),
            error: encode({
              message: 'interrupted: the process running this effect stopped',
              interrupted: true,
            } satisfies EffectError),
          },
        }
      : {
          eventType: 'step_failed',
          correlationId: record.stepId,
          eventData: {
            stepName: stepNameFor(record.name),
            error: encode({
              message:
                'interrupted: the process running this effect stopped before it reported a result, and it is not safe to rerun. It may or may not have taken effect.',
              interrupted: true,
              partial,
            } satisfies EffectError),
          },
        };
  try {
    await log.write(request);
  } catch {
    await log.refresh().catch(() => {});
  }
  // Its progress stream is finished either way (a retry is a new step with its own).
  if (partial.length) await world.streams.close(log.runId, effectStreamName(log.runId, record.stepId)).catch(() => {});
}

/** Attempts a replay-safe effect gets when its parked work keeps being lost. */
const MAX_PARKED_RETRIES = 3;

interface Drive {
  ctx: DriveContext;
  /** Whether another driver took the session from this drive. */
  preempted(): boolean;
  /** Stop watching the log for preemption. */
  dispose(): void;
  /** Why this drive was stopped from outside the harness (a run-ahead rejection, a preemption), if it was. */
  halted(): Error | undefined;
  /** The harness's effects that have not finished yet: their `fn`, then their outcome's write. */
  inFlight(): Promise<unknown>[];
  /** Stop the drive from outside (`ctx.halt`). */
  halt(reason: unknown): void;
  /** The handoff this drive committed, if it escalated. */
  handoff(): HandoffOutput | undefined;
  /** Whether other effects of this drive were still running when it committed its handoff. */
  siblingsAtHandoff(): boolean;
}

/** Who a drive is: its id, its anchor, and whether it has written anything (it then closes). */
interface DriveIdentity {
  drv: string;
  anchor: Anchor;
  unsure: { value: boolean };
  /** The crash already armed when this drive began, if any (see `ChaosDriveOptions.fire`). */
  armedAtStart?: string;
  /** When the drive's budget ends (`DriveContext.deadline`). */
  deadline?: number;
}

/**
 * Which armed crashes a driver fires (`DriverOptions.chaos`). Every driver
 * with chaos on records arming and honours what the log says (a crash that
 * fired elsewhere is not fired again); `fire` decides whether this one kills
 * its own process for one:
 *
 * - `true` (the default): every armed crash that matches an effect.
 * - `false`: none. A drive that reaches a matching effect leaves the crash
 *   armed, for a driver that fires it.
 * - `'armed-in-drive'`: only a crash armed while this drive was already
 *   running (arming one mid-turn crashes that turn where it runs); a crash
 *   already armed when the drive began is left armed. For an app that sends
 *   the turns of a session with an armed crash to a process of their own (a
 *   drill route), so a drill's kill takes down only its own session: on a
 *   platform that runs many requests in one process, a kill there takes
 *   every one of them with it.
 */
export interface ChaosDriveOptions {
  fire?: boolean | 'armed-in-drive';
}

/** The revision each document an outcome changed was published at, and the drive's rewind count then. */
type Published = Map<string, number> & { rewinds?: number };

function createContext(
  options: DriverOptions,
  log: SessionLog,
  live: JsonLinesWriter,
  budget: AbortSignal,
  attempt: number,
  identity: DriveIdentity,
  marking = false,
  hints?: HintHub,
  pipe?: PipelinedLog,
  /** The log was just handed over fresh (the submit's): the first refresh may be skipped (see `refresh`). */
  freshLog = false
): Drive {
  const { world } = options;
  /**
   * Run-ahead, on a log the submit just wrote: the harness's first refresh
   * would wait on the log's lock behind the submit's own in-flight write and
   * then list the log again, ~100 ms before the model call, to learn
   * nothing the speculative fold does not already hold. Skip that one; a
   * newer submission from elsewhere still arrives with the next refresh (the
   * inbox poll, a hint, the driver's own).
   */
  let skipRefresh = !!pipe && freshLog && attempt === 0;
  const runId = log.runId;
  const contendedDelay = options.contendedDelaySeconds ?? 2;
  const lanes = options.lanes;
  const lane = lanes ? currentLane(log.state, lanes.initial) : undefined;
  /** The fencing token this drive holds: every intent it writes carries it. */
  const epoch = log.state.epoch;
  const chaosOn = options.chaos === undefined ? chaosEnabledByEnv() : options.chaos !== false;
  const chaosFire = typeof options.chaos === 'object' ? (options.chaos.fire ?? true) : true;
  /** Whether this drive carries out drills on the session's own sandbox (they kill no process, so `'armed-in-drive'` fires them too). */
  const drillsOn = chaosOn && chaosFire !== false;
  /** What carries out those drills, registered by the harness (`ctx.drills`). */
  const drillTargets = new Set<DrillTarget>();
  /**
   * Set once this drive commits a handoff (or loses the session). The
   * session belongs to another lane from then on, so this drive must not
   * write anything else: a model or steer step committed after the handoff
   * would land in the next lane's log as a foreign write (it would yield
   * `contended`, or worse, replay a step it never asked for). Harnesses
   * whose kernels keep going for a moment after a tool throws (fx) rely on
   * this.
   */
  let handedOff = false;
  /** The handoff this drive committed (an escalation), and whether siblings were still running then. */
  let handoff: HandoffOutput | undefined;
  let siblingsAtHandoff = false;
  /**
   * Aborted, with `Yield('preempted')`, the moment this drive learns the
   * session is no longer its: a refresh (an epoch hint's, the driver's
   * periodic one, Pi's inbox poll, `ctx.refresh()`) shows the epoch moved or
   * the lane left `serves`, or a write of its own comes back fenced or
   * reports a foreign handoff. It is every effect's `io.signal`, and part of
   * `ctx.signal`.
   */
  const ownership = new AbortController();
  const preempt = (why: string) => {
    if (ownership.signal.aborted) return;
    handedOff = true;
    options.log?.('preempted', { runId, why, epoch, now: log.state.epoch });
    ownership.abort(new Yield('preempted'));
  };
  /** `ctx.halt`: stop the drive from a harness callback, with `reason` as its outcome. */
  const halt = (reason: unknown) => {
    if (ownership.signal.aborted) return;
    if (reason instanceof Yield && reason.reason === 'preempted') return preempt('halted by the harness');
    options.log?.('halted', { runId, reason: reason instanceof Error ? reason.message : String(reason) });
    ownership.abort(reason instanceof Error ? reason : new Error(String(reason)));
  };
  /**
   * The World's in-band fence refused a write of this drive (the log latched
   * it): stop now. A refusal that came with a new epoch is a preemption like
   * any other; at the same epoch it is `superseded` (see `Yield`).
   */
  const supersede = () => {
    if (ownership.signal.aborted) return;
    handedOff = true;
    if (log.state.epoch !== epoch) return preempt('the World refused a write: another driver moved the session');
    options.log?.('superseded', { runId, epoch, drv: identity.drv });
    ownership.abort(new Yield('superseded'));
  };
  const check = () => {
    if (ownership.signal.aborted) return;
    if (log.superseded) return supersede();
    if (handedOff) return;
    if (log.state.epoch !== epoch) return preempt('another driver moved the session');
    if (lanes && !lanes.serves.includes(currentLane(log.state, lanes.initial))) {
      preempt('the session left this lane');
    }
  };
  const unobserve = log.observe(check);
  const signal = AbortSignal.any([budget, ownership.signal]);
  const stopped = () => ownership.signal.reason as Yield;
  /** What the harness reads: the speculative fold when running ahead, else the durable one. */
  const view = (): SessionState => (pipe ? pipe.speculative : log.state);
  /** Documents this drive published from its speculative fold (run-ahead): a rewind publishes what the log has for them. */
  const speculativeDocs = new Set<string>();
  /** What each document's live events carry, from the definitions this drive's calls used (`DocDefinition.live`). */
  const liveOf = new Map<string, DocLive>();
  const pinned = (def: DocDefinition<any>) => liveOf.set(def.kind, def.live ?? 'value');
  const docEvent = (doc: DocState) => docLiveEvent(doc, markTime(), liveOf.get(doc.kind));
  /** Rewinds this drive has written: an outcome published before one is published again when it lands. */
  let rewinds = 0;
  /** This attempt's run-ahead chain (fold chain fence); a restart mints a new one. */
  const chain = pipe ? `ch_${ulid()}` : undefined;
  let seq = 0;
  /**
   * A run-ahead write did not take: stop this drive. The harness restarts
   * from the durable fold (`Restart`), or yields when the session is no
   * longer this drive's to restart.
   */
  const onRejected = (r: Rejection) => {
    if (r.reason === 'transport') unsure();
    if (ownership.signal.aborted) return;
    live.write({ type: 'rewind', toSlot: r.slot, cause: r.reason, t: markTime(), pid: PROCESS_ID });
    rewinds++;
    // Documents published from the speculative fold may name edits that will
    // never land: after the rewind, what the log has now (a client takes the
    // next doc event of each kind after a rewind, whatever its revision), and
    // an outcome published before it is published again once it lands.
    for (const kind of speculativeDocs) {
      const doc = log.state.docs.get(kind);
      if (doc) live.write(docEvent(doc));
    }
    speculativeDocs.clear();
    note('rewind', { cause: r.reason, slot: r.slot });
    options.log?.('run-ahead: rewind', { runId, cause: r.reason, slot: r.slot, message: r.message });
    switch (r.reason as RejectCause) {
      case 'chaos':
        return; // The process is dying; the crash path has it.
      case 'epoch':
        return preempt(`a run-ahead write came back fenced (${r.message})`);
      case 'foreign':
      case 'first-writer':
        ownership.abort(new Yield('contended', contendedDelay));
        return;
      default:
        ownership.abort(new Restart(r.message));
    }
  };
  const offRejected = pipe?.onRejected(onRejected);
  /** Wait for everything this drive issued to be durable (a barrier). */
  const barrier = async () => {
    if (!pipe) return;
    try {
      await pipe.drain();
    } catch (err) {
      if (ownership.signal.aborted) throw stopped();
      throw new Restart(`run-ahead: ${(err as Error).message}`);
    }
    if (ownership.signal.aborted) throw stopped();
  };
  /** Write through the pipeline when running ahead (awaited), else straight to the log. */
  const put = (request: CreateEventRequest, landed?: (s: SessionState) => boolean): Promise<WriteResult> =>
    pipe
      ? pipe.enqueue(request, { head: true, survives: true, ...(landed ? { landed, persistMs: RUN_AHEAD_DEMOTE_MS } : {}) })
      : log.write(request);

  /**
   * Write an outcome. If the World refuses it, find out whether we lost the
   * session. With `expect`, also make sure the outcome *took*: a recovery
   * that resolved this intent first (a delivery that found this drive quiet)
   * makes our write inert, or makes the World refuse it, and then the log
   * holds the recovery's result, not ours. The harness must never act on a
   * result the log does not have, so that is a `Restart`: it rebuilds from
   * the log and sees what the log says happened.
   */
  async function commit(
    request: CreateEventRequest,
    expect?: { key: string; stepId: string; status: 'completed' | 'failed' }
  ): Promise<WriteResult> {
    const tookElsewhere = () => {
      if (!expect) return false;
      const r = log.state.effects.get(expect.key);
      return r?.stepId !== expect.stepId || r.status !== expect.status;
    };
    const landed = () => {
      if (!expect) return false;
      const r = log.state.effects.get(expect.key);
      return r?.stepId === expect.stepId && r.status === expect.status;
    };
    let result: WriteResult | undefined;
    // An outcome write that throws may still have committed (the response
    // was lost, a relay dropped after sending). Read the log to find out
    // before reporting anything: acting on a failure that did not happen
    // would hand a `replay: 'never'` effect's caller an error for work that
    // ran and is recorded, and a caller (a model) may then run it again.
    // A write that did not land is retried: an outcome is idempotent to the
    // fold (a second completion of a completed step is inert).
    const attempts = expect ? 3 : 1;
    for (let attempt = 1; result === undefined; attempt++) {
      try {
        // Running ahead, the pipeline looks a lost response up before
        // resending: the write is in the log once our intent is not running.
        result = await put(
          request,
          expect
            ? (st) => {
                const r = st.effects.get(expect.key);
                return !(r?.stepId === expect.stepId && r.status === 'running');
              }
            : undefined
        );
      } catch (err) {
        // Refused by the World's in-band fence: nothing landed, and the
        // write is never sent again (the log has already been re-read).
        if (err instanceof LogSuperseded || (err instanceof Rejection && log.superseded)) {
          supersede();
          throw stopped();
        }
        unsure();
        const refreshed = await log.refresh().then(
          () => true,
          () => false
        );
        check();
        if (ownership.signal.aborted) throw stopped();
        if (refreshed && landed()) return { foreign: [], preempted: false };
        // Still our running intent: the write did not land. Anything else
        // (another step for the key, a different outcome) is a recovery's.
        const r = expect && log.state.effects.get(expect.key);
        const stillOurs = r && r.stepId === expect!.stepId && r.status === 'running';
        if (refreshed && !stillOurs && tookElsewhere()) throw new Restart(`effect ${expect!.key} was resolved by a recovery first`);
        if (attempt >= attempts) throw err;
        options.log?.('commit.retry', { runId, attempt, eventType: request.eventType, error: String(err) });
        await new Promise((r) => setTimeout(r, 100 * attempt));
      }
    }
    if (result.preempted) preempt('a handoff landed while this effect ran');
    if (ownership.signal.aborted) throw stopped();
    if (tookElsewhere()) throw new Restart(`effect ${expect!.key} was resolved by a recovery first`);
    return result;
  }

  /** This drive's id: every intent it writes names it, and so does a crash that kills it. */
  const { drv, anchor } = identity;
  /** A write of this drive may or may not have landed (see `deliverOpen`'s `unsure`). */
  const unsure = () => {
    identity.unsure.value = true;
  };
  const note = (name: MarkName, fields: Partial<Omit<Mark, 'type' | 'name' | 'runId'>> = {}) => {
    if (!marking || ownership.signal.aborted) return;
    writeMark(live, log, name, { ...(lane ? { lane } : {}), drv, ...fields });
  };
  /** What an effect's start and end are called in marks. */
  const phase = (name: string): ['model.request', 'model.done'] | ['tool.start', 'tool.end'] | undefined =>
    name === 'model' ? ['model.request', 'model.done'] : name.startsWith('tool.') ? ['tool.start', 'tool.end'] : undefined;

  /**
   * If an in-process crash (Kill Function) is armed for this kind of effect,
   * schedule it. The decision is atomic per effect (`gate`): the timer
   * crashes only if the effect is still `open` when it fires, and from that
   * instant the effect cannot record its result (`effect()` sees `crashing`
   * and waits for the kill). If the effect settled first, the timer backs off
   * and the crash stays armed for the next matching effect.
   *
   * A crash fences the log before anything else, so nothing this drive was
   * about to write (the next checkpoint, a result) lands between the decision
   * and the kill; then it commits a v2 `chaos` step naming the effect and this
   * drive, which proves every intent the drive left open dead at once (no
   * waiting for its delivery to come back), and kills the process.
   */
  function armChaos(key: string, stepId: string, name: string, progress: JsonLinesWriter, gate: ChaosGate): { disarm: () => void; afterMs?: number } {
    if (!chaosOn || chaosFire === false) return { disarm: () => {} };
    const armed = armedChaos(log.state);
    if (armed && isSessionDrill(armed.chaos) && chaosMatches(armed.chaos, name)) return armDrill(armed, key, stepId, name, gate);
    if (!armed || chaosKind(armed.chaos) !== 'process' || !chaosMatches(armed.chaos, name)) return { disarm: () => {} };
    // Armed before this drive began: a drive of its own fires it (see ChaosDriveOptions).
    if (chaosFire === 'armed-in-drive' && armed.id === identity.armedAtStart) return { disarm: () => {} };
    const afterMs = armed.chaos.afterMs ?? DEFAULT_CHAOS_AFTER_MS[armed.chaos.at];
    let timer: ReturnType<typeof setTimeout>;
    const fire = async () => {
      // A park write in flight: the effect is not settling, just recording
      // its owner. Look again right after it.
      if (gate.state === 'parking') {
        timer = setTimeout(fire, 50);
        return;
      }
      if (gate.state !== 'open') return;
      gate.state = 'crashing';
      const token = log.fence('chaos');
      const chaosStep = `step_${ulid()}`;
      const output: ChaosOutput = {
        v: 2,
        kind: 'process',
        crashedDuring: key,
        stepId,
        effect: name,
        afterMs,
        driver: drv,
        ...(lane ? { lane } : {}),
        ...(armed.chaos.silent ? { silent: true } : {}),
      };
      const envelope: StepEnvelope = { key: chaosKey(armed), replay: 'safe', placed: [armed.id], input: output, epoch, drv, anchor };
      let effective = true;
      try {
        await log.write(
          {
            eventType: 'step_started',
            correlationId: chaosStep,
            eventData: { stepName: stepNameFor('chaos'), input: encode(envelope) },
          },
          { fenceToken: token }
        );
        effective = log.state.effects.get(chaosKey(armed))?.stepId === chaosStep;
        if (!effective) {
          // The target was already closed in log order (a recovery resolved
          // it): this decision does not apply. Carry on, crash still armed.
          log.unfence(token);
          gate.state = 'open';
          options.log?.('chaos: the target was no longer open; not crashing', { runId, key });
          return;
        }
        await progress.flush().catch(() => {});
        await log.write(
          {
            eventType: 'step_completed',
            correlationId: chaosStep,
            eventData: { stepName: stepNameFor('chaos'), result: encode(output) },
          },
          { fenceToken: token }
        );
        live.write({ type: 'chaos', ...output, t: markTime(), pid: PROCESS_ID });
        await live.flush();
        // Hand the session to a continuation as this drive's last act, so a
        // delivery recovers it at once rather than when this delivery's lock
        // lapses (a direct drive's companion would wait out its deadline).
        // The chaos step proves the drive dead too. A drill-only shortcut:
        // a real kill sends nothing and writes no proof (`silent` drills
        // measure the queue's own redelivery). Delayed a second so the queue
        // cannot hand it back to this instance before the kill takes it down.
        if (!armed.chaos.silent) await wake(world, runId, `chaos:${chaosStep}`, 1, { continues: anchorKey(anchor) }).catch(() => {});
      } catch {
        // Could not record it: crash anyway (the queue redelivers this delivery).
      }
      options.log?.('chaos: crashing the process', { runId, key, afterMs, drv });
      // This process's record of the drive dies with it (a test's `crash`
      // returns; the drive itself never moves again).
      dropDrive(world, runId, anchorKey(anchor));
      (options.crash ?? crashProcess)();
    };
    timer = setTimeout(fire, afterMs);
    return { disarm: () => clearTimeout(timer), afterMs };
  }

  /**
   * Arm a drill on the session's own sandbox for this effect, mid-step
   * (`sandbox-vm` at a tool, `relay-drop`): if a registered target carries
   * its kind out, a timer fires it `afterMs` into the effect, as a Kill
   * Function's does, but the drive goes on. The decision is the chaos step
   * (`commitDrill`), accepted only while the effect still runs; an effect
   * that settled first leaves the drill armed for the next one. The attach
   * drills fire from their step's own body instead (`io.drill`).
   */
  function armDrill(armed: Submission & { chaos: ChaosSpec }, key: string, stepId: string, name: string, gate: ChaosGate): { disarm: () => void } {
    const kind = chaosKind(armed.chaos);
    const target = ATTACH_DRILLS.includes(kind) ? undefined : [...drillTargets].find((t) => t.kinds.includes(kind));
    if (!target) return { disarm: () => {} };
    const afterMs = armed.chaos.afterMs ?? DEFAULT_CHAOS_AFTER_MS[armed.chaos.at];
    let timer: ReturnType<typeof setTimeout>;
    const fire = async () => {
      if (gate.state === 'parking') {
        timer = setTimeout(fire, 50);
        return;
      }
      if (gate.state !== 'open' || ownership.signal.aborted) return;
      const drill = await commitDrill(armed, kind, key, stepId, name, afterMs);
      if (!drill) return;
      await target.fire(drill).catch((err) => options.log?.('chaos: the drill did not run', { runId, kind, key, error: String((err as Error)?.message ?? err) }));
    };
    timer = setTimeout(fire, afterMs);
    return { disarm: () => clearTimeout(timer) };
  }

  /**
   * Commit a drill's decision: a v2 chaos step against the running effect
   * (inert unless it still runs), which places the armed submission (fired:
   * it never fires again). It names no driver: the drive was not killed.
   * The drill, once the log accepted it; undefined otherwise (still armed).
   */
  async function commitDrill(armed: Submission & { chaos: ChaosSpec }, kind: ChaosKind, key: string, stepId: string, name: string, afterMs: number): Promise<Drill | undefined> {
    const output: ChaosOutput = { v: 2, kind, crashedDuring: key, stepId, effect: name, afterMs, ...(lane ? { lane } : {}) };
    try {
      const { won } = await commitFact(log, 'chaos', chaosKey(armed), output, { epoch, drv, anchor, placed: [armed.id] });
      if (!won) {
        options.log?.('chaos: the drill\'s step no longer runs; it stays armed', { runId, kind, key });
        return undefined;
      }
    } catch (err) {
      options.log?.('chaos: the drill could not be recorded; not run', { runId, kind, key, error: String((err as Error)?.message ?? err) });
      return undefined;
    }
    live.write({ type: 'chaos', ...output, t: markTime(), pid: PROCESS_ID });
    options.log?.(`chaos: ${kind}`, { runId, key, afterMs });
    return { kind, spec: armed.chaos, effect: name, key };
  }

  /**
   * Commit a `park` for the running intent `stepId`: an owner outside this
   * drive has it now. Arms the owner check (and an armed sandbox crash).
   */
  async function park(
    key: string,
    stepId: string,
    name: string,
    gate: ChaosGate,
    progress: JsonLinesWriter,
    owner: ParkOwner,
    parkOptions: ParkOptions = {}
  ): Promise<ParkHandle> {
    if (gate.state === 'crashing') await forever();
    // What the work reported so far (its command id) is durable before the
    // park: a drive that dies in between leaves it for the next call to find.
    await progress.flush().catch(() => {});
    const prev = gate.state;
    gate.state = 'parking';
    let result: { won: boolean; stepId: string };
    try {
      result = await commitFact(log, 'park', `park:${stepId}`, { forKey: key, forStepId: stepId, owner }, { epoch, drv, anchor });
    } catch (err) {
      if (err instanceof LogFenced) await forever();
      unsure();
      await log.refresh().catch(() => {});
      check();
      if (ownership.signal.aborted) throw stopped();
      throw err;
    } finally {
      if (gate.state === 'parking') gate.state = prev;
    }
    check();
    if (ownership.signal.aborted) throw stopped();
    if (!result.won) {
      // Our intent is not running any more (a recovery resolved it): the log
      // holds another outcome. Rebuild from it.
      throw new Restart(`effect ${key} could not be parked`);
    }
    if (parkOptions.checkAfterSeconds !== undefined) {
      await armOwnerCheck(world, runId, stepId, parkOptions.checkAfterSeconds).catch((err) =>
        options.log?.('owner: arming the check failed (a re-entry arms another)', { runId, stepId, error: String(err) })
      );
    }
    // A sandbox crash armed for this effect fires from the queue, against
    // the parked step, `afterMs` after the effect started.
    const armed = armedChaos(log.state);
    if (chaosOn && armed && chaosKind(armed.chaos) !== 'process' && chaosKind(armed.chaos) !== 'drop-callback' && !isSessionDrill(armed.chaos) && chaosMatches(armed.chaos, name)) {
      const started = log.state.effects.get(key)?.startedAt?.getTime() ?? Date.now();
      const afterMs = armed.chaos.afterMs ?? DEFAULT_CHAOS_AFTER_MS.sandbox;
      await armChaosFire(world, runId, stepId, armed.id, Math.max(0, afterMs - (Date.now() - started)) / 1000).catch(() => {});
    }
    return {
      stepId: result.stepId,
      settle: async (settlement) => {
        const outcome = await settleParked(log, { key, stepId }, settlement, { drv });
        return outcome === 'won';
      },
    };
  }

  /**
   * Re-enter an effect whose parked work settled: its intent is still
   * running, and the settlement says what happened. A failure is committed
   * as-is; a success goes through the harness's `fn` (which returns the
   * settled value in its own shape) and is committed like any outcome.
   */
  async function reenter<T>(record: EffectRecord, opts: EffectOptions, fn: (io: EffectIO) => Promise<T>): Promise<T> {
    const settlement = record.settlement!;
    const { key, stepId } = record;
    // Parked work that was lost (its VM went away under it) is an
    // interruption like any other: a replay-safe effect runs again, as a new
    // attempt (bounded, so a VM that keeps dying cannot loop a call forever).
    if (!settlement.ok && settlement.error.interrupted && opts.replay === 'safe' && record.attempt < MAX_PARKED_RETRIES) {
      await release(stepId, opts, `its parked work was lost (${settlement.error.message})`);
      await log.refresh().catch(() => {});
      return effect(key, opts, fn);
    }
    if (!settlement.ok) {
      await commit(
        {
          eventType: 'step_failed',
          correlationId: stepId,
          eventData: { stepName: stepNameFor(opts.name), error: encode(settlement.error) },
        },
        { key, stepId, status: 'failed' }
      );
      if (ownership.signal.aborted) throw stopped();
      throw new EffectFailure(settlement.error);
    }
    const docs = callDocs(view, pinned);
    const io: EffectIO = {
      signal: ownership.signal,
      stepId,
      progress: () => {},
      park: async () => {
        throw new Error(`effect ${key} is already parked`);
      },
      settlement,
      docs: docs.io,
    };
    const output = await fn(io);
    if (ownership.signal.aborted) throw stopped();
    await commit(
      {
        eventType: 'step_completed',
        correlationId: stepId,
        eventData: { stepName: stepNameFor(opts.name), result: encode(outcomeResult(output, docs.edits())) },
      },
      { key, stepId, status: 'completed' }
    );
    if (ownership.signal.aborted) throw stopped();
    publishDocs(key, stepId);
    return output;
  }

  /**
   * After an outcome that changed documents commits (under run-ahead, at
   * enqueue, like `tool_end`): one live `doc` event per document it
   * changed, with the value the fold has now. Returns the revision each was
   * published at (under run-ahead, the speculative fold's: see `correctDocs`).
   */
  function publishDocs(key: string, stepId: string): Published | undefined {
    if (ownership.signal.aborted) return undefined;
    const st = view();
    const record = st.effects.get(key);
    if (record?.stepId !== stepId || !record.docs) return undefined;
    const published: Published = new Map();
    published.rewinds = rewinds;
    for (const d of record.docs) {
      const doc = d.dropped ? undefined : st.docs.get(d.kind);
      if (!doc) continue;
      live.write(docEvent(doc));
      published.set(d.kind, doc.revision);
      if (pipe) speculativeDocs.add(d.kind);
    }
    return published;
  }

  /**
   * Under run-ahead, once an outcome's ack is folded into the durable log:
   * its edits were published from the speculative fold, which had not seen
   * what landed in between (an app's `updateDoc`). Where the log put them at
   * another revision, publish what the log has, so the last doc event a
   * client keeps (the highest revision) is the log's document, not a value
   * the log never held under that revision.
   */
  function correctDocs(st: SessionState, key: string, stepId: string, published: Published | undefined) {
    const record = st.effects.get(key);
    if (record?.stepId !== stepId || !record.docs) return;
    // A rewind since it was published told clients to drop what they had.
    const stale = published?.rewinds !== rewinds;
    for (const d of record.docs) {
      if (d.dropped || (!stale && published?.get(d.kind) === d.revision)) continue;
      const doc = st.docs.get(d.kind);
      if (doc) live.write(docEvent(doc));
    }
  }

  /** Release an intent this drive is abandoning, the way a recovery would find it. */
  async function release(stepId: string, opts: EffectOptions, why: string) {
    await commit(
      opts.replay === 'safe'
        ? {
            eventType: 'step_retrying',
            correlationId: stepId,
            eventData: {
              stepName: stepNameFor(opts.name),
              error: encode({ message: `interrupted: ${why}`, interrupted: true } satisfies EffectError),
            },
          }
        : {
            eventType: 'step_failed',
            correlationId: stepId,
            eventData: {
              stepName: stepNameFor(opts.name),
              error: encode({
                message: `interrupted: the drive stopped (${why}) before this effect reported a result, and it is not safe to rerun. It may or may not have taken effect.`,
                interrupted: true,
                partial: await readJsonLines(world, runId, effectStreamName(runId, stepId)),
              } satisfies EffectError),
            },
          }
    );
  }

  /** Whether `stepId` is the running intent for `key` in `st`. */
  function runningOurs(st: SessionState, key: string, stepId: string): boolean {
    const r = st.effects.get(key);
    return r?.stepId === stepId && r.status === 'running';
  }

  /** Decide, at a run-ahead intent's ack, whether it took. */
  function intentVerdict(key: string, stepId: string, opts: EffectOptions) {
    return (st: SessionState, result: WriteResult): RejectCause | undefined => {
      const winner = st.effects.get(key);
      if (winner?.stepId !== stepId) {
        if (st.fenced.has(stepId) || result.preempted || st.epoch !== epoch) return 'epoch';
        // Our step never happened: close it so it does not show as running
        // (written without a seq, so the chain fence does not apply to it).
        void log
          .write({
            eventType: 'step_failed',
            correlationId: stepId,
            eventData: { stepName: stepNameFor(opts.name), error: encode({ message: 'superseded' } satisfies EffectError) },
          })
          .catch(() => {});
        if (winner) return 'first-writer';
        return chain && st.fencedChains.has(chain) ? 'chain' : 'withdraw';
      }
      if (result.preempted) return 'epoch';
      if (result.foreign.length > 0) return 'foreign';
      return undefined;
    };
  }

  /** Decide, at a run-ahead outcome's ack, whether it took (a recovery may have resolved the record first). */
  function outcomeVerdict(key: string, stepId: string, status: 'completed' | 'failed') {
    return (st: SessionState, result: WriteResult): RejectCause | undefined => {
      const r = st.effects.get(key);
      if (r?.stepId === stepId && r.status === status) return undefined;
      return result.preempted || st.epoch !== epoch ? 'epoch' : 'resolved';
    };
  }

  /**
   * A run-ahead drive stopped by a rejection (not by a new owner): release
   * the intents it leaves running, the way a recovery would, so the restart
   * (or the next delivery) finds them released instead of contended.
   */
  /**
   * A run-ahead drive stopped by a rejection (not by a new owner) whose
   * effect `fn` finished anyway: write that real outcome while the intent is
   * still ours and running. Returns whether the outcome is (now) recorded.
   */
  async function recordSurvivor(key: string, stepId: string, request: CreateEventRequest): Promise<boolean> {
    if (!pipe) return false;
    const reason = ownership.signal.reason;
    // A drive that lost the session (or whose writes the World refuses) records nothing.
    if (reason instanceof Yield && (reason.reason === 'preempted' || reason.reason === 'superseded')) return false;
    await pipe.settled();
    if (!runningOurs(log.state, key, stepId) || log.state.effects.get(key)?.park) return false;
    const verdict = outcomeVerdict(key, stepId, request.eventType === 'step_completed' ? 'completed' : 'failed');
    try {
      await pipe.enqueue(request, {
        survives: true,
        persistMs: RUN_AHEAD_DEMOTE_MS,
        // Its edits were never published (the drive stopped first): once they land, what the log has.
        check: (st, result) => {
          const cause = verdict(st, result);
          if (cause === undefined) correctDocs(st, key, stepId, undefined);
          return cause;
        },
        landed: (st) => !runningOurs(st, key, stepId),
      });
      return true;
    } catch {
      return !runningOurs(log.state, key, stepId);
    }
  }

  async function releaseIfOurs(stepId: string, opts: EffectOptions) {
    if (!pipe) return;
    const reason = ownership.signal.reason;
    if (reason instanceof Yield && (reason.reason === 'preempted' || reason.reason === 'superseded')) return;
    await pipe.settled();
    const r = log.state.effectOrder.find((e) => e.stepId === stepId);
    if (!r || r.status !== 'running' || log.state.effects.get(r.key) !== r || r.park) return;
    await release(stepId, opts, `run-ahead rewind: ${(reason as Error)?.message ?? 'rejected'}`).catch(() => {});
  }

  async function effect<T>(
    key: string,
    opts: EffectOptions,
    fn: (io: EffectIO) => Promise<T>
  ): Promise<T> {
    const record = view().effects.get(key);
    if (record?.status === 'completed') return record.output as T;
    if (record?.status === 'failed') throw new EffectFailure(record.error!);
    // Ownership, before every effect, against the latest fold: a drive
    // that lost the session stops here, before it writes anything.
    check();
    if (ownership.signal.aborted) throw stopped();
    if (handedOff) throw new Yield('handoff');
    if (record?.status === 'running') {
      // Parked and not settled: the sandbox has it. Stop; its report wakes us.
      if (record.park && !record.settlement) {
        handedOff = true;
        // This drive was delivered while the step is parked (a restart, a
        // lost wake, a redelivery): it is detached by now (a recovery
        // detaches a parker it proved gone), and an owner check must stand
        // behind it, in case whatever armed the first one did not survive.
        if (!record.detached) await commitDetach(log, record, { drv, anchor }).catch(() => {});
        await armOwnerCheck(world, runId, record.stepId, 1, `re:${drv}`).catch(() => {});
        throw new Yield('parked');
      }
      if (record.settlement) return reenter(record, opts, fn);
      if (pipe && record.driver === drv && !pipe.inFn(record.stepId)) {
        // Our own intent, from an earlier attempt of this drive, whose `fn`
        // is not running any more and whose outcome did not land (the
        // restart re-issued what it could): nobody else will resolve it.
        // Release it now, the way a recovery would, and go on.
        await release(record.stepId, opts, 'its result was lost to a write failure');
        await log.refresh().catch(() => {});
        return effect(key, opts, fn);
      }
      throw new Yield('contended', contendedDelay);
    }
    if (budget.aborted) throw budget.reason;
    // Run ahead: a replay-safe effect starts at once, its intent durable
    // later. Anything else is a barrier: everything issued so far is durable
    // (and took) before it starts. Past the backpressure thresholds every
    // effect waits.
    const ahead = !!pipe && opts.replay === 'safe';
    if (pipe) {
      if (pipe.failed) await barrier();
      if (!ahead || pipe.pressured()) await barrier();
    }

    // Escalate before committing an intent this lane cannot carry out.
    // Capability routing: run here, or hand the session to the
    // lane that can. The decision is a `handoff` fact; replay never re-derives it.
    const needs = needsOf(opts);
    const target =
      lanes && lane && needs.length > 0
        ? route(needs, lane, (l) => (lanes.available ? lanes.available(l, log.state) : true))
        : undefined;
    if (lanes && lane && target) {
      const move: HandoffOutput = {
        from: lane,
        to: target,
        reason: opts.reason ?? `${opts.name} needs the ${target} lane`,
        forKey: key,
      };
      // The handoff moves the epoch: whatever this drive issued must land first.
      await barrier();
      handedOff = true;
      // Other effects of this drive may still be running (parallel tool
      // calls): the handoff names this drive, so they stay its own until it
      // closes, and the receiving lane leaves them be.
      siblingsAtHandoff = running.size > 1;
      let won: boolean;
      try {
        ({ won } = await commitHandoff(log, move, epoch, { drv, anchor }));
      } catch (err) {
        if (err instanceof LogSuperseded) {
          supersede();
          throw stopped();
        }
        throw err;
      }
      if (!won) {
        // Someone else moved the session first (a takeover): theirs stands.
        preempt('lost a handoff race');
        throw stopped();
      }
      // The session is the next lane's: the outcomes this drive still writes
      // (siblings of the escalation, its release, its close) are no longer
      // in-band, so they never fence the next lane's drive.
      log.disarm(drv);
      handoff = move;
      live.write({ type: 'lane', ...move });
      void publishHint(world, runId, { eventType: 'handoff', slot: log.state.eventCount, to: move.to, epoch: log.state.epoch });
      note('handoff.sent', { ...move });
      // The mover wakes the next lane before it lets go of its delivery.
      await wakeLane(options, runId, lanes, move);
      throw new Yield('handoff');
    }

    // Commit the intent: always a new born-running step, fenced by this
    // drive's epoch. A retry of a released effect names the step it retries.
    const stepId = `step_${ulid()}`;
    const envelope: StepEnvelope = {
      key,
      replay: opts.replay,
      ...(opts.placed?.length ? { placed: opts.placed } : {}),
      input: opts.input,
      epoch,
      ...(opts.requires ? { requires: opts.requires } : {}),
      ...(opts.needs?.length ? { needs: opts.needs } : {}),
      ...(record?.status === 'pending' ? { retryOf: record.stepId } : {}),
      drv,
      anchor,
      ...(chain ? { chain, seq: ++seq } : {}),
    };
    const intent: CreateEventRequest = {
      eventType: 'step_started',
      correlationId: stepId,
      eventData: { stepName: stepNameFor(opts.name), input: encode(envelope) },
    };
    let foreign: Event[] = [];
    let preempted = false;
    if (ahead) {
      // First-writer and epoch checks move to the ack (`intentVerdict`).
      pipe!.enqueue(intent, { check: intentVerdict(key, stepId, opts), landed: (st) => stepKnown(st, stepId) });
    } else {
      try {
        ({ foreign, preempted } = await (pipe ? pipe.enqueue(intent, { landed: (st) => stepKnown(st, stepId) }) : log.write(intent)));
      } catch (err) {
        // Refused by the in-band fence: the intent never landed, so its effect never starts.
        if (err instanceof LogSuperseded || log.superseded) {
          supersede();
          throw stopped();
        }
        unsure();
        if (ownership.signal.aborted) throw stopped();
        throw err;
      }
    }
    const winner = ahead ? view().effects.get(key) : log.state.effects.get(key);
    if (!ahead && winner?.stepId !== stepId) {
      if (log.state.fenced.has(stepId) || preempted || log.state.epoch !== epoch) {
        // Written after a handoff took the session: nobody will ever read
        // it, and this drive must write nothing more.
        preempt('an intent came back fenced');
        throw stopped();
      }
      // Our step never happened: we lost the first-writer-wins race for this
      // key to another driver, or placed a submission withdrawn ahead of us.
      await log.write({
        eventType: 'step_failed',
        correlationId: stepId,
        eventData: {
          stepName: stepNameFor(opts.name),
          error: encode({ message: 'superseded' } satisfies EffectError),
        },
      });
      if (winner) throw new Yield('contended', contendedDelay);
      throw new Restart(`effect ${key} placed a withdrawn submission`);
    }
    if (preempted) {
      // A handoff landed right after our intent: the new owner resolves it.
      preempt('a handoff landed with this intent');
      throw stopped();
    }
    if (foreign.length > 0) {
      // Another driver is making progress on this session. Let it have it,
      // and release our intent so it does not sit `running` with no owner.
      await release(stepId, opts, 'another driver is live').catch(() => {});
      throw new Yield('contended', contendedDelay);
    }

    const progress = new JsonLinesWriter(
      world,
      runId,
      effectStreamName(runId, stepId)
    );
    const gate: ChaosGate = { state: 'open' };
    let chaos: ReturnType<typeof armChaos> = { disarm: () => {} };
    // Documents, pinned per call: the edits ride on this call's outcome.
    const docs = callDocs(view, pinned);
    const io: EffectIO = {
      signal: ownership.signal,
      stepId,
      docs: docs.io,
      progress: (chunk) => {
        progress.write(chunk);
      },
      park: async (owner, parkOptions) => {
        // Parking hands the work to an owner outside this drive: a barrier.
        if (ahead) await barrier();
        return park(key, stepId, opts.name, gate, progress, owner, parkOptions);
      },
      get chaosArmedMs() {
        return chaos.afterMs;
      },
      ...(drillsOn
        ? {
            drill: async (kinds: readonly ChaosKind[]) => {
              if (ownership.signal.aborted) return undefined;
              const armed = armedChaos(log.state);
              if (!armed || !isSessionDrill(armed.chaos) || !kinds.includes(chaosKind(armed.chaos)) || !chaosMatches(armed.chaos, opts.name)) return undefined;
              // A fact about this step lands after its intent.
              if (ahead) await barrier();
              return commitDrill(armed, chaosKind(armed.chaos), key, stepId, opts.name, 0);
            },
          }
        : {}),
    };
    chaos = armChaos(key, stepId, opts.name, progress, gate);
    note('intent', { key, effect: opts.name, ...(opts.placed?.length ? { placed: opts.placed } : {}) });
    const marks = phase(opts.name);
    if (marks) note(marks[0], { key, effect: opts.name, ...(opts.placed?.length ? { placed: opts.placed } : {}) });
    const parked = () => {
      const r = log.state.effects.get(key);
      return r?.stepId === stepId && r.status === 'running' && r.park ? r : undefined;
    };
    let output: T;
    // When the work itself ended (the model's response, the tool's return),
    // before its outcome is committed. The end mark (`model.done`,
    // `tool.end`) carries this time, so a reader timing "the model" or "the
    // tool" does not get the commit write with it: that write is the
    // harness's, and counts as overhead (as eve's step persistence does).
    let endedAt = 0;
    const endFields = (ok: boolean) => ({ key, effect: opts.name, ok, t: endedAt, commitMs: Math.round(markTime() - endedAt) });
    const leaveFn = pipe?.enterFn(stepId, opts.replay !== 'safe');
    try {
      output = await fn(io);
      endedAt = markTime();
    } catch (err) {
      endedAt = markTime();
      leaveFn?.();
      // A crash was decided while this ran: its result must not be recorded.
      if (gate.state === 'crashing') await forever();
      gate.state = 'settling';
      chaos.disarm();
      await progress.flush();
      // Lost the session mid-effect: whatever `fn` threw (an abort, a Yield)
      // is not this effect's outcome, and the new owner resolves the intent.
      if (ownership.signal.aborted) {
        await releaseIfOurs(stepId, opts);
        throw stopped();
      }
      let p = parked();
      if (p && !p.settlement) {
        // A collector (a push, an owner check) may have settled the step
        // while this drive was still attached. Take it here rather than stop
        // and leave it to a wake: the collector saw the step attached and
        // did not start another drive.
        await log.refresh().catch(() => {});
        check();
        if (ownership.signal.aborted) throw stopped();
        p = parked();
        if (p?.settlement) return reenter(p, opts, fn);
      }
      if (p && !p.settlement && !p.detached) {
        // Let go of it. The log orders this against a settlement: if one
        // landed first, the detach is inert and this drive takes it here;
        // if the detach landed first, the settler wakes a drive.
        const { won } = await commitDetach(log, p, { drv, anchor }).catch(() => ({ won: false }));
        if (!won) {
          await log.refresh().catch(() => {});
          check();
          if (ownership.signal.aborted) throw stopped();
          p = parked();
          if (p?.settlement) return reenter(p, opts, fn);
        }
      }
      if (p && !p.settlement) {
        // The work runs on outside this drive (it said so, or the drive is
        // stopping for another reason while it runs): never release it.
        handedOff = true;
        throw new Yield('parked');
      }
      if (p?.settlement && !p.settlement.ok) {
        // Settled as a failure (the VM was lost, a collector saw it first):
        // that settlement is the outcome, whatever `fn` threw.
        await commit(
          {
            eventType: 'step_failed',
            correlationId: stepId,
            eventData: { stepName: stepNameFor(opts.name), error: encode(p.settlement.error) },
          },
          { key, stepId, status: 'failed' }
        );
        if (ownership.signal.aborted) throw stopped();
        throw new EffectFailure(p.settlement.error);
      }
      if (p && (err instanceof Yield || err instanceof Restart)) throw err;
      // Settled as a success before this drive's `fn` stopped (it parked,
      // or failed on its own way out): the settlement is the outcome, never
      // the park or the error. Committed in the harness's own shape.
      if (p?.settlement?.ok) return reenter(p, opts, fn);
      if (err instanceof Yield || err instanceof Restart) {
        // The drive is stopping, not the effect failing: an effect a harness
        // abandons as it halts (a model stream cut off by a budget yield, a
        // steer that lost the session) must not be recorded as a failure,
        // or every later delivery would rethrow it. Release the intent the
        // way the next delivery would find it abandoned, and stop.
        await release(stepId, opts, err.message);
        throw err;
      }
      const error: EffectError =
        err instanceof EffectInterrupted
          ? {
              message: err.message,
              interrupted: true,
              partial: await readJsonLines(world, runId, effectStreamName(runId, stepId)),
            }
          : { message: err instanceof Error ? err.message : String(err) };
      await commit(
        {
          eventType: 'step_failed',
          correlationId: stepId,
          eventData: { stepName: stepNameFor(opts.name), error: encode(error) },
        },
        { key, stepId, status: 'failed' }
      );
      void progress.close();
      if (marks) note(marks[1], endFields(false));
      if (ownership.signal.aborted) throw stopped();
      throw new EffectFailure(error);
    }
    leaveFn?.();
    // A crash was decided while this ran: its result must not be recorded.
    if (gate.state === 'crashing') await forever();
    gate.state = 'settling';
    chaos.disarm();
    // Progress is UI and interruption evidence: a success does not wait for it.
    if (pipe) void progress.flush().catch(() => {});
    else await progress.flush();
    const completed: CreateEventRequest = {
      eventType: 'step_completed',
      correlationId: stepId,
      eventData: { stepName: stepNameFor(opts.name), result: encode(outcomeResult(output, docs.edits())) },
    };
    if (ownership.signal.aborted) {
      // `fn` returned: this is the effect's real result. A drive stopped by
      // a run-ahead rejection elsewhere (a sibling's write, not a new
      // owner) still owns this intent, so it records the result as a
      // survivor instead of releasing it as "may or may not have taken
      // effect", which would invite a caller to run it again.
      if (!(await recordSurvivor(key, stepId, completed))) await releaseIfOurs(stepId, opts);
      throw stopped();
    }
    const settled = parked()?.settlement;
    if (settled && !settled.ok) {
      await commit(
        {
          eventType: 'step_failed',
          correlationId: stepId,
          eventData: { stepName: stepNameFor(opts.name), error: encode(settled.error) },
        },
        { key, stepId, status: 'failed' }
      );
      if (ownership.signal.aborted) throw stopped();
      throw new EffectFailure(settled.error);
    }
    let published: Published | undefined;
    if (pipe) {
      // Run ahead: the harness has the result now. The outcome of an effect
      // that must not run twice jumps the queue (the window where its real
      // result can be lost is one write); the next barrier waits for it.
      const verdict = outcomeVerdict(key, stepId, 'completed');
      pipe.enqueue(completed, {
        head: !ahead,
        check: (st, result) => {
          const cause = verdict(st, result);
          if (cause === undefined) correctDocs(st, key, stepId, published);
          return cause;
        },
        landed: (st) => !runningOurs(st, key, stepId),
        survives: (st) => runningOurs(st, key, stepId),
        // The harness already acts on this result: a blip must not lose it.
        // Idempotent (an outcome of our committed intent, looked up before
        // every resend), so resend for a while, and if it is still lost the
        // restart re-issues it before replaying.
        ...(ahead ? {} : { persistMs: RUN_AHEAD_DEMOTE_MS, reissue: true }),
      });
    } else {
      await commit(completed, { key, stepId, status: 'completed' });
    }
    published = publishDocs(key, stepId);
    // Settled: its progress stream ends (once its lines land; a success does not wait for it).
    void progress.close();
    if (marks) note(marks[1], endFields(true));
    if (ownership.signal.aborted) throw stopped();
    return output;
  }

  // The harness's effects until each settles. A harness may stop on
  // `signal` without waiting for a tool it started (the budget aborts it):
  // the driver finishes those inside the margin before it wakes the session.
  const running = new Set<Promise<void>>();
  const tracked: typeof effect = (key, opts, fn) => {
    const p = effect(key, opts, fn);
    const done = p.then(
      () => {},
      () => {}
    );
    running.add(done);
    void done.then(() => running.delete(done));
    return p;
  };
  const ctx: DriveContext = {
    runId,
    world,
    config: log.state.config,
    signal,
    state: view,
    effect: tracked,
    isMemoized: (key) => {
      const status = view().effects.get(key)?.status;
      return status === 'completed' || status === 'failed';
    },
    pending: () => pendingSubmissions(view()),
    refresh: async () => {
      if (skipRefresh) {
        skipRefresh = false;
        if (pipe!.lag.pending > 0) return;
      }
      await log.refresh();
    },
    live: (event) => {
      if (ownership.signal.aborted) return;
      live.write(event);
    },
    mark: (name, fields) => note(name, fields),
    halt,
    ...(hints
      ? {
          onHint: (listener: (hint: Hint) => void) => {
            const off = hints.on(listener);
            signal.addEventListener('abort', off, { once: true });
            return off;
          },
        }
      : {}),
    attempt,
    ...(lane ? { lane } : {}),
    docs: docsReader(view),
    fence: () => log.fenceOf(identity.drv),
    ...(identity.deadline !== undefined ? { deadline: identity.deadline } : {}),
    ...(drillsOn
      ? {
          drills: (target: DrillTarget) => {
            drillTargets.add(target);
            const off = () => void drillTargets.delete(target);
            signal.addEventListener('abort', off, { once: true });
            return off;
          },
        }
      : {}),
  };
  return {
    ctx,
    preempted: () => {
      const reason = ownership.signal.reason;
      return ownership.signal.aborted && reason instanceof Yield && reason.reason === 'preempted';
    },
    halted: () => (ownership.signal.aborted ? (ownership.signal.reason as Error) : undefined),
    inFlight: () => [...running],
    halt,
    handoff: () => handoff,
    siblingsAtHandoff: () => siblingsAtHandoff,
    dispose: () => {
      unobserve();
      offRejected?.();
    },
  };
}

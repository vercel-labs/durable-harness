import type { EffectRecord, SessionState, Submission } from './types.ts';

/**
 * Crash injection, for proving durability on a real platform.
 *
 * Arming a crash is an inbox submission (`kind: 'chaos'`), so it lives in
 * the session's log like everything else: it survives redelivery, and the
 * UI can show it. The next effect that matches `at` and is still running
 * `afterMs` after it started gets a crash: the driver records a `chaos`
 * step naming the effect it killed, then kills the process for real
 * (`process.exit` in a function, a reload in a tab). Nothing about recovery
 * is simulated: the platform sees a crashed invocation and the queue
 * redrives it.
 */
export interface ChaosSpec {
  /**
   * Which effect to crash during: a tool call, a model stream, either, or a
   * machine command (`sandbox`: hybrid's `run_on_machine`, or the older
   * `sandbox_exec`; the only target of the parked kinds; a `process` crash
   * also targets a sandbox harness's `sandbox.prepare`). The attach drills
   * (`sandbox-new-vm`, `sandbox-zombie`) fire at the attach of the session's
   * sandbox whatever this says (`sandbox` by convention).
   */
  at: 'tool' | 'model' | 'any' | 'sandbox';
  /** What dies. Default `process`. */
  kind?: ChaosKind;
  /** How long the effect runs before the crash. */
  afterMs?: number;
  /**
   * `process` only: die the way a real platform kill does, leaving no proof
   * of death. The decision is still recorded (so the crash fires once), but
   * marked `silent`, so nothing treats it as evidence the drive is dead and
   * no continuation is sent: recovery is the queue's own (the dead
   * delivery's lock lapses and its message comes back), which is what an
   * OOM or an instance loss costs.
   */
  silent?: boolean;
}

/**
 * What dies. The parked kinds target a `sandbox_exec` that runs detached
 * (`at: 'sandbox'`, a hybrid session); the session kinds target the session's
 * own Vercel Sandbox (`sandbox()`: a VM per session, its Drive at
 * `/workspace`), and fire only where the harness registered a drill target
 * for them (`DriveContext.drills`, `EffectIO.drill`): the sandbox tier's
 * attach does (docs/workspaces.md §8 "Drills").
 *
 * - `process` (Kill Function): the process driving the effect, SIGKILL. In a
 *   session on its own sandbox (D5) the relay kills the CLI when the link
 *   goes silent, and the next driver arms the VM with a higher fence.
 * - `sandbox-vm` (Kill Sandbox VM): the VM a parked command runs on is
 *   stopped (`at: 'sandbox'`); or (`at: 'tool'`, D1) the session's VM is
 *   stopped mid-call: the drive re-attaches on a new VM on the same Drive and
 *   the call comes back interrupted, not rerun.
 * - `sandbox-command` (Kill Command): the parked command is killed; its wrapper reports.
 * - `drop-callback` (Drop Callback): the command finishes, its report is discarded.
 * - `sandbox-new-vm` (Restart on a New Sandbox, D2): at the next attach of the
 *   session's VM (a turn's start), the VM is deleted and the Drive kept; the
 *   turn boots a new VM on the same Drive.
 * - `sandbox-zombie` (Zombie VM, D3): at the next attach, the old VM is
 *   stopped and left holding the Drive; the new driver finds it, deletes it
 *   once it stopped, waits out the Drive's 409 and resumes.
 * - `relay-drop` (Drop the Relay, D4): one of the relay link's sockets is
 *   dropped mid-step (`/model` during a model stream, else `/ctl`): the drive
 *   re-attaches in the same invocation, no failed turn.
 */
export type ChaosKind = 'process' | 'sandbox-vm' | 'sandbox-command' | 'drop-callback' | 'sandbox-new-vm' | 'sandbox-zombie' | 'relay-drop';

export const chaosKind = (spec: ChaosSpec): ChaosKind => spec.kind ?? 'process';

/** The kinds that fire at the attach of the session's sandbox (`EffectIO.drill`), not mid-step. */
export const ATTACH_DRILLS: readonly ChaosKind[] = ['sandbox-new-vm', 'sandbox-zombie'];

/**
 * Whether `spec` is a drill on the session's own sandbox: carried out by a
 * drill target the harness registered (`DriveContext.drills`, mid-step; or
 * `EffectIO.drill`, at the attach), not by the driver's kill or the parked
 * command's owner.
 */
export function isSessionDrill(spec: ChaosSpec): boolean {
  const kind = chaosKind(spec);
  return kind === 'relay-drop' || ATTACH_DRILLS.includes(kind) || (kind === 'sandbox-vm' && spec.at !== 'sandbox');
}

/** A drill on the session's sandbox, as its target carries it out: the kind, the spec it was armed with, and the effect it fired in. */
export interface Drill {
  kind: ChaosKind;
  spec: ChaosSpec;
  /** The effect's step name (`tool.Bash`, `model`, `sandbox.attach`) and memo key. */
  effect: string;
  key: string;
}

/**
 * What carries out the drills on a session's own sandbox, registered for one
 * drive (`DriveContext.drills`). The kernel arms each one like a Kill
 * Function, `afterMs` into the next matching step: once its chaos step is
 * committed (accepted only while that step still runs), it calls `fire`.
 * Unlike a kill, the drive goes on: the step ends however the drill leaves
 * it (interrupted, when the VM went away under it).
 */
export interface DrillTarget {
  /** The kinds it carries out mid-step (`sandbox-vm` at a tool, `relay-drop`). */
  readonly kinds: readonly ChaosKind[];
  fire(drill: Drill): Promise<void>;
}

/**
 * What a `chaos` step records. Version 2 is written at the intent (the step
 * takes effect when created, like a handoff) and is accepted by the fold only
 * if its target is still running and unsettled at that point in the log, so
 * a crash decision can never name an effect that had already finished.
 */
export interface ChaosOutput {
  v?: 2;
  kind?: ChaosKind;
  /** The memo key of the effect that was running when it died. */
  crashedDuring: string;
  /** The target's step id (v2). */
  stepId?: string;
  effect: string;
  afterMs: number;
  /** The drive that was killed (`process`): every intent it left open is dead. */
  driver?: string;
  lane?: string;
  cmdId?: string;
  /** A silent kill: recorded so it fires once, never proof of death. */
  silent?: boolean;
}

export const DEFAULT_CHAOS_AFTER_MS = { tool: 1500, model: 1500, any: 1500, sandbox: 4000 } as const;

export const chaosKey = (submission: Submission) => `chaos:${submission.id}`;

/**
 * The oldest armed crash that has not fired yet, if any. A crash fires by
 * committing a `chaos` step that places its submission, so "fired" is a
 * pure fold of the log like everything else.
 */
export function armedChaos(state: SessionState): (Submission & { chaos: ChaosSpec }) | undefined {
  return state.submissions.find(
    (s): s is Submission & { chaos: ChaosSpec } =>
      s.kind === 'chaos' && !!s.chaos && !state.placed.has(s.id) && !state.withdrawn.has(s.id)
  );
}

/** The chaos record's payload: its output, or (a v2 step not yet finished) its intent. */
export function chaosOutputOf(record: EffectRecord): ChaosOutput | undefined {
  if (record.name !== 'chaos') return undefined;
  const out = (record.output ?? record.input) as ChaosOutput | undefined;
  return out && typeof out === 'object' && 'crashedDuring' in out ? out : undefined;
}

/** Whether a committed (or half-written) v2 process crash names this drive. */
export function chaosKilledDrive(state: SessionState, drv: string | undefined): boolean {
  if (!drv) return false;
  return state.effectOrder.some((e) => {
    if (e.name !== 'chaos' || (e.status !== 'completed' && e.status !== 'running')) return false;
    const out = chaosOutputOf(e);
    return out?.v === 2 && !out.silent && (out.kind ?? 'process') === 'process' && out.driver === drv;
  });
}

/**
 * Whether the log itself proves the process running `record` died (a `gone`
 * verdict, anchor.ts), so nothing waits for its delivery to come back:
 *
 * - a v2 `process` crash named the drive that wrote the intent: that whole
 *   invocation was killed, whatever else it had open (a checkpoint it began
 *   writing as the crash was decided, a model call);
 * - (legacy) a `chaos` step naming the effect's key committed after its intent.
 */
export function crashedByChaos(
  state: SessionState,
  record: Pick<EffectRecord, 'key' | 'startedAt' | 'driver'>
): boolean {
  return state.effectOrder.some((e) => {
    if (e.name !== 'chaos' || (e.status !== 'completed' && e.status !== 'running')) return false;
    const out = chaosOutputOf(e);
    if (!out) return false;
    if (out.v === 2) return !out.silent && (out.kind ?? 'process') === 'process' && !!out.driver && out.driver === record.driver;
    return (
      e.status === 'completed' &&
      out.crashedDuring === record.key &&
      (!record.startedAt || !e.startedAt || e.startedAt >= record.startedAt)
    );
  });
}

export function chaosMatches(spec: ChaosSpec, effectName: string): boolean {
  if (effectName === 'chaos' || effectName === 'handoff' || effectName === 'recover' || effectName === 'fs') return false;
  if (effectName === 'park' || effectName === 'settle' || effectName === 'detach' || effectName === 'close' || effectName === 'host') return false;
  const kind = chaosKind(spec);
  // The attach drills fire where the session's sandbox is attached: a turn's start.
  if (ATTACH_DRILLS.includes(kind)) return effectName === 'sandbox.attach';
  if (spec.at === 'sandbox') return effectName === 'tool.run_on_machine' || effectName === 'tool.sandbox_exec' || (kind === 'process' && effectName === 'sandbox.prepare');
  // The parked kinds only ever target a sandbox command; a stopped session VM (D1) a tool call.
  if (kind === 'sandbox-command' || kind === 'drop-callback') return false;
  if (kind === 'sandbox-vm') return spec.at === 'tool' && effectName.startsWith('tool.');
  if (spec.at === 'any') return effectName === 'model' || effectName.startsWith('tool.');
  if (spec.at === 'model') return effectName === 'model';
  return effectName.startsWith('tool.');
}

/**
 * Kill this invocation the way a platform failure would. SIGKILL, not
 * `process.exit`: a host can intercept `exit` (and exit handlers run JS),
 * which let a dying function record a half-finished tool result on Vercel.
 * Nothing runs after a SIGKILL.
 */
export function crashProcess(): never {
  const proc = (globalThis as {
    process?: { pid?: number; kill?: (pid: number, signal: string) => void; exit?: (code: number) => never };
  }).process;
  if (typeof proc?.kill === 'function' && proc.pid) proc.kill(proc.pid, 'SIGKILL');
  if (typeof proc?.exit === 'function') proc.exit(137);
  const loc = (globalThis as { location?: { reload(): void } }).location;
  loc?.reload();
  throw new Error('harness-world chaos: crashed');
}

export function chaosEnabledByEnv(): boolean {
  const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env;
  return env?.HARNESS_CHAOS === '1';
}

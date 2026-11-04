import { type DriveContext, EffectFailure, type EffectRecord, Restart, Unavailable } from 'harness-world';
import type { WorkspaceMachine } from 'harness-world-hands/runtime';
import type { SessionMachine } from './environment.ts';
import { clip } from './tools.ts';

/**
 * The app's own setup of a session's Drive, for a harness that runs its CLI
 * in the session's sandbox (Claude Code, Codex): `prepare` puts what the
 * first turn needs on the Drive (a repository, its install, the app's own
 * files) before the model starts, with credentials that never reach the
 * transcript or the model's commands.
 *
 * It runs in the drive, right after the attach and before anything else
 * runs on the machine, once per Drive: on the session's first attach, and
 * again when the Drive is a new one (its files were lost: `driveId` changed).
 * Each run is a kernel effect (`sandbox.prepare`, replay `safe`):
 *
 * - a replay skips it (its outcome is in the log);
 * - a crash, or a lost link to the VM, while it runs runs it again, on what
 *   the interrupted run left on the Drive: `prepare` must be idempotent;
 * - a failure (it threw) is committed and fails the turn with an
 *   `Unavailable`; the session is kept, and its next message runs `prepare`
 *   again (a new attempt).
 */

/** What `prepare` gets. */
export interface DriveToPrepare {
  /**
   * The session's `/workspace`. On the sandbox tier's machine its commands'
   * `env` may carry credentials (`SessionMachine.provision`): pass a token to
   * the one command that needs it, keep it out of the files it writes, and
   * don't leave a process running (the session's processes are killed when
   * `prepare` returns).
   */
  workspace: WorkspaceMachine;
  /** The Drive's id: `prepare` runs once per Drive. Undefined on a machine with no Drive record. */
  driveId: string | undefined;
  /** Aborted when the drive stops or the link to the VM is lost. */
  signal: AbortSignal;
}

/** The hook: see `DriveToPrepare`. */
export type PrepareDrive = (drive: DriveToPrepare) => Promise<void>;

/** The effect's step name. */
export const PREPARE_STEP = 'sandbox.prepare';

/** The memo key prefix of a Drive's prepare runs (`prepare:<driveId>#<attempt>`). */
export const prepareKeyPrefix = (driveId: string | undefined) => `prepare:${driveId ?? 'drive'}#`;

/**
 * Run `prepare` on `machine`'s Drive if no run of it completed for this Drive.
 * Returns the effect's key when it ran in this drive (the Drive may have
 * changed), else undefined. Call it right after the attach, before anything
 * spawns on the machine.
 */
export async function prepareDrive(ctx: DriveContext, machine: SessionMachine, prepare: PrepareDrive | undefined, options: { harness: string }): Promise<string | undefined> {
  if (!prepare) return undefined;
  const prefix = prepareKeyPrefix(machine.driveId);
  const runs = new Map<string, EffectRecord>();
  for (const e of ctx.state().effectOrder) if (e.key.startsWith(prefix)) runs.set(e.key, e);
  if ([...runs.values()].some((e) => e.status === 'completed')) return undefined;
  // A run a crash interrupted is taken up again under its own key; after a failure, the next attempt has its own.
  const key = [...runs.values()].find((e) => e.status !== 'failed')?.key ?? `${prefix}${runs.size + 1}`;
  try {
    await ctx.effect(key, { name: PREPARE_STEP, replay: 'safe', input: { driveId: machine.driveId ?? null } }, async (io) => {
      const signal = AbortSignal.any([io.signal, machine.signal]);
      const run = (workspace: WorkspaceMachine) => prepare({ workspace, driveId: machine.driveId, signal });
      try {
        await (machine.provision ? machine.provision(run) : run(machine.workspace));
      } catch (err) {
        // A lost link is the driver's to re-attach from (the run starts again on the new attach), not the hook's failure.
        if (machine.signal.aborted && !io.signal.aborted) throw new Restart("the link to the session's sandbox was lost while its Drive was prepared");
        throw err;
      }
      return null;
    });
  } catch (err) {
    if (err instanceof EffectFailure) {
      throw new Unavailable(`The session's Drive could not be prepared: ${clip(err.error.message, 500)}`, {
        hint: `${options.harness}'s prepare threw. The session is kept, and its next message runs prepare again on this Drive.`,
      });
    }
    throw err;
  }
  return key;
}

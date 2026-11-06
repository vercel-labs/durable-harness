import type { DeliverMeta } from './driver.ts';
import type { SessionLog } from './session-log.ts';

/**
 * In-process bookkeeping for the drive route: a Function
 * that just accepted a submission or a handoff drives the session itself,
 * after its response, instead of a queue hop to the flow route. The drive
 * holds no queue message: it runs under a deadline anchor (anchor.ts), and
 * the companion its submit sent, delayed past that deadline, is its backstop.
 * On a World whose queue holds a message for its sender (hold.ts), the
 * submit's wake is held instead and runs here as that message's delivery;
 * the drive this route starts waits for it (`DurableSessions.drive`).
 *
 * One drive per session per process is the kernel's (`deliver`): a drive
 * that lands while the session drives here acks, and the drive under way
 * runs once more before it lets go.
 */
export interface DriveRouteOptions {
  drive(runId: string, meta: DeliverMeta): Promise<{ timeoutSeconds: number } | undefined>;
  /** Where the drive runs after the response (Next's `after`, Vercel's `waitUntil`). */
  after(work: () => Promise<void>): void;
  log?: (message: string, fields?: Record<string, unknown>) => void;
}

export interface DriveRequest {
  why: string;
  /** Runs first, under `after`: return false to skip the drive, or a log to start the drive from. */
  prepare?: () => Promise<false | { log?: SessionLog } | undefined>;
  meta?: DeliverMeta;
}

export interface DriveRoute {
  /** Drive `runId` here after the response. False if this process already drives it (it then runs again). */
  drive(runId: string, request: DriveRequest): boolean;
  driving(runId: string): boolean;
}

export function createDriveRoute(options: DriveRouteOptions): DriveRoute {
  const driving = new Map<string, number>();
  const say = options.log ?? (() => {});

  async function run(runId: string, request: DriveRequest) {
    const t0 = Date.now();
    try {
      const meta: DeliverMeta = { ...request.meta };
      if (request.prepare) {
        const prepared = await request.prepare();
        if (prepared === false) return;
        if (prepared?.log) meta.log = prepared.log;
      }
      await options.drive(runId, meta);
      say('drive route: done', { runId, why: request.why, ms: Date.now() - t0 });
    } catch (error) {
      say('drive route: failed', { runId, why: request.why, error: String(error) });
    } finally {
      const n = (driving.get(runId) ?? 1) - 1;
      if (n > 0) driving.set(runId, n);
      else driving.delete(runId);
    }
  }

  return {
    drive(runId, request) {
      const already = driving.has(runId);
      driving.set(runId, (driving.get(runId) ?? 0) + 1);
      options.after(() => run(runId, request));
      return !already;
    },
    driving: (runId) => driving.has(runId),
  };
}

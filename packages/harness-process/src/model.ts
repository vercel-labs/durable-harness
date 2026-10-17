import { randomUUID } from 'node:crypto';
import { armedChaos, chaosKind, chaosMatches, type DriveContext, EffectFailure, type SessionState } from 'harness-world';
import { gatewayModelOptions, gatewayModelResult, isModelRequest } from './agent-runs.ts';
import type { TrackedRequest, TrackEnd } from './gateway.ts';

/**
 * Model steps: a record-only `model` effect around a process harness's model
 * requests, so a "Crash during: model" drill has a step to fire in.
 *
 * The kernel's chaos hook kills a drive from inside an effect. A process
 * harness's model call is made by its child (the CLI), not by the drive, so
 * it was never an effect, and a crash armed for the model had nothing to
 * fire in. Every process harness already sends its child's model traffic
 * through `gatewayProxy`, so the drive sees each model request start and
 * end there: pass `track: steps.request` to the proxy.
 *
 * A model step is written only around a drill:
 *
 * - while a process crash (or a dropped relay, `relay-drop`) that targets
 *   the model (`at: 'model'` or `'any'`) is armed: the step opens when a model request starts and closes when the
 *   last model request in flight ends (overlapping requests share one step).
 *   The crash fires `afterMs` into it, as into a Pi model step: the time to
 *   the first token counts, and with these CLIs' large system prompts that
 *   is often 1 to 3 s, so a short delay can kill the call before anything
 *   streamed. Timing it from the stream instead was tried: the Gateway
 *   sometimes delivers a whole reply in a burst, and the crash then never
 *   fired.
 * - when a crash left a model step unfinished: the next model request (the
 *   recovered turn regenerating the response) retries that step, so the log
 *   says the call was made again (attempt 2), and nothing is left open.
 *
 * Otherwise nothing is written: recovery never needs a model step. The
 * child's own session, mirrored into the journal, is what a resume reads,
 * and a model response a crash cut off is regenerated (and billed again).
 *
 * The step's input is the request's method and path, its output how many
 * requests it covered and how the last one ended. One `ModelSteps` per
 * drive; `close()` before the drive returns, so no step outlives it.
 */

export interface ModelStepsOptions {
  /** Diagnostics: a step write that failed. */
  log?: (message: string, fields?: Record<string, unknown>) => void;
}

/** What a model step records as its output. */
export interface ModelStepOutput {
  /** Model requests the step covered (overlapping requests share one step). */
  requests: number;
  /** The last request's status, or how it ended. */
  status?: number;
  error?: string;
  /**
   * A crash left the step unfinished and no model request of a later drive
   * made the call again (its turn had already ended, or the harness resumed
   * without it): closed so nothing is left open. It covered no request.
   */
  abandoned?: true;
}

/** Whether a request is a model call: a POST (not a token count). */
export { isModelRequest } from './agent-runs.ts';

/** A crash armed in `state` would fire in a model step: a Kill Function, or a dropped relay on the session's sandbox (D4: `/model` dropped mid-stream). */
export function modelCrashArmed(state: SessionState): boolean {
  const armed = armedChaos(state);
  const kind = armed ? chaosKind(armed.chaos) : undefined;
  return !!armed && (kind === 'process' || kind === 'relay-drop') && chaosMatches(armed.chaos, 'model');
}

/**
 * The key of a model step a crash left unfinished (the kernel's recovery
 * released it, `pending`), oldest first, skipping `taken`: the next model
 * request retries it.
 */
export function unfinishedModelStep(state: SessionState, taken: ReadonlySet<string> = new Set()): string | undefined {
  return state.effectOrder.find((e) => e.name === 'model' && e.status === 'pending' && !taken.has(e.key))?.key;
}

/**
 * Close every model step a crash left unfinished (`pending`) that no model
 * request of this drive retried (`taken`): the call it covered will not be
 * made again. A crash can kill a drive after its turn's end is in the log
 * but before its model step's outcome is (a CLI's last request ends, the
 * turn ends, and the process dies before the step's write lands); the next
 * drive finds the turn finished, so its CLI never asks for that response
 * again, and the step would stay open forever: the session idle "with work
 * the harness did not take", its last step still running. Each is committed
 * as `abandoned` (a model step is record-only: nothing reruns). Call it when
 * a drive is done, also on the path that starts no child. Returns how many.
 */
export async function settleAbandonedModelSteps(ctx: DriveContext, taken: ReadonlySet<string> = new Set()): Promise<number> {
  const seen = new Set(taken);
  let n = 0;
  for (let key = unfinishedModelStep(ctx.state(), seen); key && !ctx.signal.aborted; key = unfinishedModelStep(ctx.state(), seen)) {
    seen.add(key);
    const input = ctx.state().effectOrder.find((e) => e.key === key)?.input ?? {};
    try {
      await ctx.effect<ModelStepOutput>(key, { name: 'model', replay: 'safe', input }, async () => ({ requests: 0, abandoned: true }));
      n++;
    } catch (err) {
      if (err instanceof EffectFailure || ctx.signal.aborted) continue;
      throw err;
    }
  }
  return n;
}

interface OpenStep {
  requests: number;
  last: { status?: number; error?: string };
  done: () => void;
}

export class ModelSteps {
  /** Model requests in flight, recorded or not. */
  private inflight = 0;
  /** Model requests seen by this drive: each one's marks are keyed by its ordinal. */
  private ordinal = 0;
  /** The step covering the requests in flight, if one is recorded. */
  private step: OpenStep | undefined;
  /** Keys this drive used: a retry is attempted once. */
  private readonly used = new Set<string>();
  private readonly running = new Set<Promise<void>>();
  private closed = false;

  constructor(
    private readonly ctx: DriveContext,
    private readonly options: ModelStepsOptions = {}
  ) {}

  /**
   * For `gatewayProxy({ track })`: a model request started; returns its end.
   * Every model request is also marked on the session's live stream
   * (`model.request`, `model.first_token` when the proxy sees its first
   * output, `model.done`), as a Pi or fx model step is, so a page can take
   * the model's own time out of the turn: the CLI's model call is not a step.
   */
  readonly request = (request: TrackedRequest): TrackEnd | undefined => {
    if (!isModelRequest(request) || this.closed) return undefined;
    // Agent Runs: each request is a model call, a step of its own, with what was asked and answered.
    const traced = this.ctx.tracing?.modelCall(gatewayModelOptions(request)).catch(() => undefined);
    const key = `request:${this.ordinal++}`;
    this.ctx.mark?.('model.request', { key });
    this.inflight++;
    if (this.step) this.step.requests++;
    else this.step = this.open(request);
    let over = false;
    let first = false;
    const end: TrackEnd = (outcome) => {
      if (over) return;
      over = true;
      const recording = traced?.then(async (call) => {
        if (!call) return;
        const failed = outcome.error ?? (outcome.status !== undefined && outcome.status >= 400 ? `the model request failed (${outcome.status})` : undefined);
        if (failed !== undefined) return call.model.fail(new Error(failed));
        const { result, attributes } = gatewayModelResult(request.path, outcome.response ?? '');
        call.attempt.attributes(attributes);
        await call.model.complete({ outcome: 'completed', result });
      }).catch(() => {});
      if (recording) {
        this.running.add(recording);
        void recording.then(() => this.running.delete(recording));
      }
      this.ctx.mark?.('model.done', { key, ...(outcome.status !== undefined ? { status: outcome.status } : {}), ...(outcome.error ? { error: outcome.error } : {}) });
      this.inflight--;
      const step = this.step;
      if (!step) return;
      step.last = { status: outcome.status, error: outcome.error };
      if (this.inflight > 0) return;
      // The step is over: a request that starts now gets a step of its own.
      this.step = undefined;
      step.done();
    };
    return Object.assign(end, {
      capture: this.ctx.tracing !== undefined,
      firstToken: () => {
        if (first || over) return;
        first = true;
        this.ctx.mark?.('model.first_token', { key });
      },
    });
  };

  /**
   * The drive is done with its turns: close the model steps a crash left
   * unfinished that no request of this drive retried (see
   * `settleAbandonedModelSteps`). Call it at the drive's normal end, before
   * `close()`.
   */
  settleAbandoned(): Promise<number> {
    return settleAbandonedModelSteps(this.ctx, this.used);
  }

  /**
   * No more steps. Resolves once every step this drive opened has its
   * outcome (or was released), or after `waitMs` (default 10 s): a step a
   * chaos crash killed never settles, and its process is about to die.
   */
  async close(waitMs = 10_000): Promise<void> {
    this.closed = true;
    const step = this.step;
    this.step = undefined;
    step?.done();
    if (!this.running.size) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const remaining = Math.max(0, Math.min(waitMs, (this.ctx.deadline ?? Infinity) - Date.now()));
    await Promise.race([Promise.allSettled([...this.running]), new Promise<void>((r) => (timer = setTimeout(r, remaining)))]);
    clearTimeout(timer);
  }

  private open(request: { method: string; path: string }): OpenStep | undefined {
    const { ctx } = this;
    const signal = ctx.signal;
    if (signal.aborted) return undefined;
    const state = ctx.state();
    const retry = unfinishedModelStep(state, this.used);
    if (!retry && !modelCrashArmed(state)) return undefined;
    const key = retry ?? `model:${randomUUID()}`;
    this.used.add(key);
    let done!: () => void;
    const finished = new Promise<void>((r) => (done = r));
    const step: OpenStep = { requests: 1, last: {}, done };
    const running: Promise<void> = ctx
      .effect<ModelStepOutput>(key, { name: 'model', replay: 'safe', input: { method: request.method, path: request.path } }, async () => {
        // The drive stopping is not the step's outcome: the kernel releases it.
        await new Promise<void>((resolve, reject) => {
          if (signal.aborted) return reject(signal.reason);
          const stop = () => reject(signal.reason);
          signal.addEventListener('abort', stop, { once: true });
          void finished.then(() => {
            signal.removeEventListener('abort', stop);
            resolve();
          });
        });
        return { requests: step.requests, ...step.last };
      })
      .then(
        () => {},
        (err) => {
          if (err instanceof EffectFailure || signal.aborted) return;
          // A write that failed or was refused: this drive cannot go on (as for a tool call).
          this.options.log?.('harness-world-process: a model step failed', { key, error: String((err as Error)?.message ?? err) });
          ctx.halt?.(err);
        }
      )
      .finally(() => this.running.delete(running));
    this.running.add(running);
    return step;
  }
}

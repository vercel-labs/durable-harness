import { monotonicFactory } from 'ulid';
import { EffectFailure, Fatal, Restart, Yield } from './driver.ts';
import { emptyState, pendingSubmissions } from './fold.ts';
import {
  callDocs,
  type DeepReadonly,
  type Doc,
  DocConflict,
  type DocLive,
  docLiveEvent,
  docsHookToken,
  docsReader,
  type DocSnapshot,
  type DocState,
  deepFreeze,
  encodeEdit,
  foldInBandEdits,
  foldOutOfBandEdits,
  materializeDoc,
  prepareUpdate,
  storedBytes,
  readDoc,
} from './docs.ts';
import { type Mark, type MarkName, mark } from './marks.ts';
import { workflowNameFor } from './names.ts';
import { effectIdempotencyKey } from './idempotency.ts';
import type { StartOptions, UpdateDocOptions } from './sessions.ts';
import type {
  DriveContext,
  EffectError,
  EffectIO,
  EffectOptions,
  EffectRecord,
  Harness,
  SessionState,
  Submission,
} from './types.ts';

/**
 * Durability off: the same harness adapters, driven with no World at all.
 *
 * A raw session keeps its "log" in memory: `effect()` runs the work and
 * records the outcome in an in-memory `SessionState` shaped exactly like the
 * fold of a durable session, so the adapters (Pi, fx) and the UI's projection
 * of the state are the same code in both modes. Nothing is awaited but the
 * work itself: no intent, outcome, stream or queue write exists. This is the
 * baseline the benchmark measures durable lanes against,
 * and what the demo's "Durability: Off" runs.
 *
 * What is lost is exactly what durability buys: a crash loses the session,
 * nothing resumes, and the session cannot move between lanes. A snapshot
 * (`snapshot`/`restore`) carries the in-memory state between stateless
 * invocations (a Function that runs one turn per request).
 */

const ulid = monotonicFactory();

export interface RawSessionsOptions {
  harness: Harness | Harness[];
  /** Write timing marks to the live stream. Default true. */
  marks?: boolean;
  log?: (message: string, fields?: Record<string, unknown>) => void;
  /** How many times one busy period may restart the harness (`Restart`). Default 8. */
  maxRestarts?: number;
}

/** A synthetic log row: what a durable session would have written, kept in memory only. */
export interface RawEvent {
  eventId: string;
  eventType: string;
  correlationId?: string;
  stepName?: string;
  createdAt: string;
}

/** A raw session's state, serializable (dates as epoch ms). */
export interface RawSnapshot {
  v: 1;
  runId: string;
  harness: string;
  config?: unknown;
  submissions: (Omit<Submission, 'receivedAt'> & { receivedAt: number })[];
  effects: (Omit<EffectRecord, 'startedAt'> & { startedAt?: number })[];
  events: RawEvent[];
  status: SessionState['status'];
  /** Documents (docs.ts), as the fold holds them. Absent in snapshots taken before documents. */
  docs?: DocState[];
}

export interface RawSession {
  id: string;
  harness: string;
  state: SessionState;
  events: RawEvent[];
  createdAt: Date;
}

export interface RawSessions {
  start(options?: StartOptions): Promise<{ sessionId: string }>;
  send(sessionId: string, text: string, kind?: 'prompt' | 'steer' | 'followUp'): Promise<string>;
  inspect(sessionId: string): Promise<SessionState>;
  /** The synthetic log (what would have been written), for the UI's event list. */
  events(sessionId: string): RawEvent[];
  list(): RawSession[];
  /** Resolves when the session has nothing left to do (no drive running, nothing pending). */
  idle(sessionId: string): Promise<void>;
  /** Live events, replayed from `startIndex` (default 0) and then as they happen. */
  live(sessionId: string, options?: { startIndex?: number; signal?: AbortSignal }): AsyncGenerator<unknown>;
  /** Call `fn` for every live event from now on. */
  subscribe(sessionId: string, fn: (event: unknown) => void): () => void;
  /** A document, migrated to this definition (see `DurableSessions.doc`). */
  doc<T extends object>(sessionId: string, doc: Doc<T>): Promise<DocSnapshot<T>>;
  /** An out-of-band edit, applied at once (see `DurableSessions.updateDoc`). */
  updateDoc<T extends object>(sessionId: string, doc: Doc<T>, mutate: (draft: T) => void | T, options?: Pick<UpdateDocOptions, 'expectRevision'>): Promise<DocSnapshot<T>>;
  snapshot(sessionId: string): RawSnapshot;
  /** Recreate a session from a snapshot (a stateless invocation continuing it). */
  restore(snapshot: RawSnapshot): string;
  /** Forget a session. */
  close(sessionId: string): void;
}

interface Entry extends Omit<RawSession, 'state'> {
  state: SessionState;
  harnessImpl: Harness;
  history: unknown[];
  listeners: Set<(event: unknown) => void>;
  driving?: Promise<void>;
  idleWaiters: (() => void)[];
}

/** The in-memory state a snapshot describes (what `restore` continues from, and what a UI shows). */
export function stateFromSnapshot(snapshot: RawSnapshot): SessionState {
  const state = emptyState(snapshot.runId);
  state.workflowName = workflowNameFor(snapshot.harness);
  state.status = snapshot.status;
  state.config = snapshot.config;
  state.inbox = { hookId: `hook_${snapshot.runId}`, token: `harness:${snapshot.runId}:inbox` };
  for (const s of snapshot.submissions) state.submissions.push({ ...s, receivedAt: new Date(s.receivedAt) });
  for (const r of snapshot.effects) {
    const rec: EffectRecord = { ...r, ...(r.startedAt !== undefined ? { startedAt: new Date(r.startedAt) } : {}) } as EffectRecord;
    // A record left running by an invocation that stopped mid-effect is lost work: run it again.
    if (rec.status === 'running') rec.status = 'pending';
    state.effectOrder.push(rec);
    state.effects.set(rec.key, rec);
    if (rec.status !== 'pending') for (const id of rec.placed) state.placed.add(id);
  }
  for (const d of snapshot.docs ?? []) state.docs.set(d.kind, { ...d, value: deepFreeze(d.value) });
  if (snapshot.docs?.length) state.docsHook = { hookId: `hook_${snapshot.runId}_docs`, token: docsHookToken(snapshot.runId) };
  state.eventCount = snapshot.events.length;
  return state;
}

export function rawSessions(options: RawSessionsOptions): RawSessions {
  const harnesses = [options.harness].flat();
  const sessions = new Map<string, Entry>();
  const marking = options.marks ?? true;
  const say = options.log ?? (() => {});

  const get = (id: string): Entry => {
    const entry = sessions.get(id);
    if (!entry) throw new Error(`harness-world raw: no session ${id}`);
    return entry;
  };

  function emit(entry: Entry, event: unknown) {
    entry.history.push(event);
    for (const fn of entry.listeners) {
      try {
        fn(event);
      } catch {}
    }
  }

  function note(entry: Entry, name: MarkName, fields: Partial<Omit<Mark, 'type' | 'name' | 'runId'>> = {}) {
    if (marking) emit(entry, mark(entry.id, name, { durableSlot: entry.state.eventCount, raw: true, ...fields }));
  }

  function record(entry: Entry, eventType: string, correlationId?: string, stepName?: string) {
    entry.state.eventCount++;
    entry.events.push({
      eventId: `raw_${String(entry.state.eventCount).padStart(6, '0')}`,
      eventType,
      ...(correlationId ? { correlationId } : {}),
      ...(stepName ? { stepName } : {}),
      createdAt: new Date().toISOString(),
    });
  }

  function create(harness: Harness, runId: string, config: unknown): Entry {
    const state = emptyState(runId);
    state.workflowName = workflowNameFor(harness.name);
    state.status = 'running';
    state.config = config;
    state.inbox = { hookId: `hook_${ulid()}`, token: `harness:${runId}:inbox` };
    const entry: Entry = {
      id: runId,
      harness: harness.name,
      harnessImpl: harness,
      state,
      events: [],
      createdAt: new Date(),
      history: [],
      listeners: new Set(),
      idleWaiters: [],
    };
    sessions.set(runId, entry);
    return entry;
  }

  function context(entry: Entry, attempt: number, drv: string, stop: AbortController): DriveContext {
    const { state } = entry;
    const effect = async <T>(key: string, opts: EffectOptions, fn: (io: EffectIO) => Promise<T>): Promise<T> => {
      const existing = state.effects.get(key);
      if (existing?.status === 'completed') return existing.output as T;
      if (existing?.status === 'failed') throw new EffectFailure(existing.error!);
      // Halted (`ctx.halt`): nothing more runs.
      if (stop.signal.aborted) throw stop.signal.reason;
      const stepId = `step_${ulid()}`;
      const rec: EffectRecord = {
        key,
        stepId,
        name: opts.name,
        replay: opts.replay,
        input: opts.input,
        status: 'running',
        attempt: (existing?.attempt ?? 0) + 1,
        startedAt: new Date(),
        startedEpoch: 0,
        placed: opts.placed ?? [],
        ...(opts.requires ? { requires: opts.requires } : {}),
        driver: drv,
      };
      if (existing) {
        const at = state.effectOrder.indexOf(existing);
        state.effectOrder[at] = rec;
      } else state.effectOrder.push(rec);
      state.effects.set(key, rec);
      for (const id of rec.placed) state.placed.add(id);
      record(entry, 'step_started', stepId, opts.name);
      const phase = opts.name === 'model' ? (['model.request', 'model.done'] as const) : opts.name.startsWith('tool.') ? (['tool.start', 'tool.end'] as const) : undefined;
      const placed = opts.placed?.length ? { placed: opts.placed } : {};
      note(entry, 'intent', { key, effect: opts.name, drv, ...placed });
      if (phase) note(entry, phase[0], { key, effect: opts.name, drv, ...placed });
      const live = new Map<string, DocLive>();
      const docs = callDocs(() => state, (def) => live.set(def.kind, def.live ?? 'value'));
      const io: EffectIO = {
        signal: new AbortController().signal,
        stepId,
        idempotencyKey: () => effectIdempotencyKey(entry.id, key),
        progress: () => {},
        park: async () => {
          throw new Error('harness-world raw: parking needs a World (durability is off)');
        },
        docs: docs.io,
      };
      try {
        const output = await fn(io);
        const edits = docs.edits();
        rec.status = 'completed';
        rec.output = output;
        record(entry, 'step_completed', stepId, opts.name);
        if (edits.length > 0) {
          // As the fold applies an outcome's envelope, at its slot.
          rec.docs = foldInBandEdits(state, edits, state.eventCount, { step: stepId, key });
          for (const d of rec.docs) {
            const doc = d.dropped ? undefined : state.docs.get(d.kind);
            if (doc) emit(entry, docLiveEvent(doc, Date.now(), live.get(d.kind)));
          }
        }
        if (phase) note(entry, phase[1], { key, effect: opts.name, drv, ok: true });
        return output;
      } catch (err) {
        if (err instanceof Yield || err instanceof Restart || err instanceof Fatal) {
          // The drive stopping: release the record, as a durable drive would.
          rec.status = 'pending';
          throw err;
        }
        const error: EffectError = { message: err instanceof Error ? err.message : String(err) };
        rec.status = 'failed';
        rec.error = error;
        record(entry, 'step_failed', stepId, opts.name);
        if (phase) note(entry, phase[1], { key, effect: opts.name, drv, ok: false });
        throw new EffectFailure(error);
      }
    };
    return {
      runId: entry.id,
      world: undefined as never,
      config: state.config,
      state: () => state,
      effect,
      isMemoized: (key) => {
        const status = state.effects.get(key)?.status;
        return status === 'completed' || status === 'failed';
      },
      pending: () => pendingSubmissions(state),
      refresh: async () => {},
      live: (event) => emit(entry, event),
      mark: (name, fields) => note(entry, name, { drv, ...fields }),
      signal: stop.signal,
      halt: (reason) => {
        if (!stop.signal.aborted) stop.abort(reason instanceof Error ? reason : new Error(String(reason)));
      },
      attempt,
      docs: docsReader(() => state),
    };
  }

  /** Drive until nothing is pending: one busy period, like a durable delivery. */
  function kick(entry: Entry) {
    if (entry.driving) return;
    entry.driving = (async () => {
      const max = options.maxRestarts ?? 8;
      try {
        while (entry.state.status === 'running' && pendingSubmissions(entry.state).length > 0) {
          const drv = `drv_${ulid()}`;
          note(entry, 'drive.start', { drv });
          let attempt = 0;
          for (;;) {
            const stop = new AbortController();
            try {
              await entry.harnessImpl.drive(context(entry, attempt, drv, stop));
              if (stop.signal.aborted) throw stop.signal.reason;
              break;
            } catch (thrown) {
              // A halted drive's reason is its outcome (see `DriveContext.halt`).
              const err = stop.signal.aborted && !(thrown instanceof Fatal) ? stop.signal.reason : thrown;
              if (err instanceof Restart && ++attempt < max) continue;
              if (err instanceof Yield) {
                say('raw: the harness yielded; nothing resumes it without a World', { runId: entry.id, reason: err.reason });
                return;
              }
              const message = err instanceof Error ? err.message : String(err);
              entry.state.status = 'failed';
              entry.state.failure = { message };
              record(entry, 'run_failed');
              emit(entry, { type: 'failed', message });
              return;
            }
          }
          note(entry, 'idle', { drv });
        }
      } finally {
        entry.driving = undefined;
        for (const w of entry.idleWaiters.splice(0)) w();
      }
    })();
  }

  function submit(entry: Entry, kind: Submission['kind'], text: string): string {
    const id = `sub_${ulid()}`;
    entry.state.submissions.push({ id, kind, text, receivedAt: new Date() });
    record(entry, 'hook_received', entry.state.inbox!.hookId);
    note(entry, 'submit', { id, kind, lagMs: 0 });
    kick(entry);
    return id;
  }

  return {
    async start(startOptions = {}) {
      const harness = startOptions.harness ? harnesses.find((h) => h.name === startOptions.harness) : harnesses[0];
      if (!harness) throw new Error(`harness-world raw: unknown harness "${startOptions.harness}"`);
      const entry = create(harness, `raw_${ulid()}`, startOptions.config);
      record(entry, 'run_created');
      record(entry, 'run_started');
      record(entry, 'hook_created', entry.state.inbox!.hookId);
      if (startOptions.prompt) submit(entry, 'prompt', startOptions.prompt);
      return { sessionId: entry.id };
    },
    async send(sessionId, text, kind = 'prompt') {
      return submit(get(sessionId), kind, text);
    },
    async inspect(sessionId) {
      return get(sessionId).state;
    },
    events: (sessionId) => get(sessionId).events,
    list: () => [...sessions.values()],
    idle(sessionId) {
      const entry = get(sessionId);
      if (!entry.driving) return Promise.resolve();
      return new Promise((resolve) => entry.idleWaiters.push(resolve));
    },
    async *live(sessionId, liveOptions = {}) {
      const entry = get(sessionId);
      const queue: unknown[] = entry.history.slice(liveOptions.startIndex ?? 0);
      let wakeUp: (() => void) | undefined;
      const fn = (event: unknown) => {
        queue.push(event);
        wakeUp?.();
      };
      entry.listeners.add(fn);
      const onAbort = () => wakeUp?.();
      liveOptions.signal?.addEventListener('abort', onAbort, { once: true });
      try {
        for (;;) {
          while (queue.length) yield queue.shift();
          if (liveOptions.signal?.aborted) return;
          await new Promise<void>((resolve) => {
            wakeUp = resolve;
          });
          wakeUp = undefined;
        }
      } finally {
        entry.listeners.delete(fn);
        liveOptions.signal?.removeEventListener('abort', onAbort);
      }
    },
    subscribe(sessionId, fn) {
      const entry = get(sessionId);
      entry.listeners.add(fn);
      return () => entry.listeners.delete(fn);
    },
    async doc(sessionId, doc) {
      return readDoc(get(sessionId).state, doc);
    },
    async updateDoc<T extends object>(sessionId: string, doc: Doc<T>, mutate: (draft: T) => void | T, updateOptions: Pick<UpdateDocOptions, 'expectRevision'> = {}): Promise<DocSnapshot<T>> {
      const entry = get(sessionId);
      const { state } = entry;
      const def = doc.definition;
      const base = materializeDoc(def, state.docs.get(def.kind));
      if (updateOptions.expectRevision !== undefined && updateOptions.expectRevision !== base.revision) {
        throw new DocConflict(def.kind, base.revision, updateOptions.expectRevision);
      }
      const edit = encodeEdit(def, base, prepareUpdate(def, base.value, mutate, { storedBytes: () => storedBytes(base) }), { exact: true });
      if (!edit) return { value: base.value as unknown as DeepReadonly<T>, revision: base.revision };
      if (!state.docsHook) {
        state.docsHook = { hookId: `hook_${entry.id}_docs`, token: docsHookToken(entry.id) };
        record(entry, 'hook_created', state.docsHook.hookId);
      }
      const id = `dedit_${ulid()}`;
      record(entry, 'hook_received', state.docsHook.hookId);
      foldOutOfBandEdits(state, { $docs: 1, id, docs: [edit] }, state.eventCount);
      const applied = state.docs.get(def.kind)!;
      emit(entry, docLiveEvent(applied, Date.now(), def.live));
      return readDoc(state, doc);
    },
    snapshot(sessionId) {
      const { id, harness, state, events } = get(sessionId);
      return {
        v: 1,
        runId: id,
        harness,
        ...(state.config !== undefined ? { config: state.config } : {}),
        ...(state.docs.size > 0 ? { docs: [...state.docs.values()] } : {}),
        status: state.status,
        submissions: state.submissions.map((s) => ({ ...s, receivedAt: s.receivedAt.getTime() })),
        effects: state.effectOrder.map(({ startedAt, ...r }) => ({ ...r, ...(startedAt ? { startedAt: startedAt.getTime() } : {}) })),
        events: [...events],
      };
    },
    restore(snapshot) {
      const harness = harnesses.find((h) => h.name === snapshot.harness);
      if (!harness) throw new Error(`harness-world raw: unknown harness "${snapshot.harness}"`);
      const entry = create(harness, snapshot.runId, snapshot.config);
      entry.state = stateFromSnapshot(snapshot);
      entry.events.push(...snapshot.events);
      return entry.id;
    },
    close(sessionId) {
      sessions.delete(sessionId);
    },
  };
}

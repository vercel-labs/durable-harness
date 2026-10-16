import { createHash } from 'node:crypto';
import type { World } from '@workflow/world';
import type { TraceCheckpointer, TraceSnapshot } from '@vercel/agent-runs';
import { decodeLine, encodeLine, WRITE_DEADLINE_MS } from 'harness-world/streams';

/** Each new line is self-contained, so recovery reads one chunk instead of the turn's history. */
interface Line { version: 1; v?: TraceSnapshot | null; drive: number }
interface State { value?: TraceSnapshot; drives: number; finished: boolean }
interface Turn extends State {
  runId: string;
  stream: string;
  loaded?: Promise<void>;
  writing: Promise<void>;
  pending?: { value: TraceSnapshot | null };
  scheduled: boolean;
  storing: number;
  unavailable?: unknown;
}
const CACHE_LIMIT = 128;
const READ_LIMIT = 16 * 1024 * 1024;

function parseKey(key: string): { runId: string; turnId: string } | undefined {
  const [kind, runId, turnId] = key.split(':');
  if (kind !== 'turn' || runId === undefined || turnId === undefined) return undefined;
  try { return { runId: decodeURIComponent(runId), turnId: decodeURIComponent(turnId) }; }
  catch { return undefined; }
}

export function checkpointStreamName(runId: string, turnId: string): string {
  return `${runId}_agent-runs_${createHash('sha256').update(turnId).digest('hex').slice(0, 24)}`;
}

/** Best-effort checkpoints: bounded I/O, ordered per turn, and no shared writer that can stall other sessions. */
export class TraceStreams {
  private readonly turns = new Map<string, Turn>();
  private readonly deadlines = new Map<string, number>();

  constructor(
    private readonly world: World,
    private readonly onError: (error: unknown) => void,
    private readonly timeoutMs = WRITE_DEADLINE_MS
  ) {}

  setDeadline(runId: string, deadline: number | undefined): void {
    if (deadline === undefined) this.deadlines.delete(runId);
    else this.deadlines.set(runId, deadline);
  }

  readonly checkpointer: TraceCheckpointer = {
    get: async (key) => {
      const turn = this.turn(key);
      if (!turn) return undefined;
      await this.load(turn);
      return turn.value;
    },
    set: (key, value) => this.write(key, value),
    delete: (key) => this.write(key, null),
  };

  forget(runId: string, turnId?: string): void {
    const only = turnId === undefined ? undefined : `turn:${encodeURIComponent(runId)}:${encodeURIComponent(turnId)}`;
    for (const [key, turn] of this.turns) {
      if (turn.runId === runId && (only === undefined || key === only) && !turn.pending && turn.storing === 0) this.turns.delete(key);
    }
  }

  async nextDrive(runId: string, turnId: string): Promise<number> {
    const turn = this.turn(`turn:${encodeURIComponent(runId)}:${encodeURIComponent(turnId)}`)!;
    return this.increment(turn);
  }

  async nextSequence(runId: string): Promise<number> {
    const key = `sequence:${encodeURIComponent(runId)}`;
    let turn = this.turns.get(key);
    if (turn && !turn.pending && turn.storing === 0) { this.turns.delete(key); turn = undefined; }
    turn ??= this.cache(key, runId, `${runId}_agent-runs_sequence`);
    return (await this.increment(turn)) - 1;
  }

  private increment(turn: Turn): Promise<number> {
    const next = turn.writing.then(async () => {
      await this.load(turn);
      if (turn.unavailable) throw turn.unavailable;
      const drive = ++turn.drives;
      await this.put(turn, turn.value, turn.finished);
      return drive;
    });
    turn.storing++;
    turn.writing = next.then(() => {}, () => {}).finally(() => { turn.storing--; });
    return this.bounded(turn, () => next);
  }

  async finished(runId: string, turnId: string): Promise<boolean> {
    const turn = this.turn(`turn:${encodeURIComponent(runId)}:${encodeURIComponent(turnId)}`)!;
    await this.load(turn);
    return turn.finished;
  }

  /** Only writes already queued for this run, not future writes or another session's traffic. */
  async flush(runId?: string): Promise<void> {
    const turns = [...this.turns.values()].filter((turn) => runId === undefined || turn.runId === runId);
    await Promise.all(turns.map(async (turn) => {
      if (turn.storing === 0 && !turn.pending) return;
      const writing = turn.writing;
      try { await this.bounded(turn, () => writing); }
      catch { /* Checkpoints never keep a drive alive past its budget. */ }
    }));
    for (const [key, turn] of this.turns) if (turn.finished && !turn.pending && turn.storing === 0) this.turns.delete(key);
  }

  private report(error: unknown): void {
    try { this.onError(error); } catch {}
  }

  private async bounded<T>(turn: Turn, work: (signal: AbortSignal) => Promise<T>): Promise<T> {
    const remaining = Math.min(this.timeoutMs, (this.deadlines.get(turn.runId) ?? Infinity) - Date.now());
    if (remaining <= 0) {
      turn.unavailable ??= new Error('Agent Runs checkpoint deadline reached');
      throw turn.unavailable;
    }
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        Promise.resolve().then(() => work(abort.signal)),
        new Promise<never>((_, reject) => { timer = setTimeout(() => {
          const error = new Error('Agent Runs checkpoint I/O timed out');
          abort.abort(error);
          reject(error);
        }, remaining); }),
      ]);
    } catch (error) {
      turn.unavailable = error;
      this.report(error);
      throw error;
    } finally { clearTimeout(timer); }
  }

  private turn(key: string): Turn | undefined {
    const turn = this.turns.get(key);
    if (turn) return turn;
    const parsed = parseKey(key);
    if (!parsed) return undefined;
    return this.cache(key, parsed.runId, checkpointStreamName(parsed.runId, parsed.turnId));
  }

  private cache(key: string, runId: string, stream: string): Turn {
    const turn: Turn = { runId, stream, drives: 0, finished: false, writing: Promise.resolve(), scheduled: false, storing: 0 };
    this.turns.set(key, turn);
    for (const [oldKey, old] of this.turns) {
      if (this.turns.size <= CACHE_LIMIT) break;
      if (old !== turn && old.storing === 0 && !old.pending) this.turns.delete(oldKey);
    }
    return turn;
  }

  private async load(turn: Turn): Promise<void> {
    if (turn.unavailable) throw turn.unavailable;
    turn.loaded ??= this.bounded(turn, async (signal) => {
      const state = await this.read(turn, signal);
      if (!turn.pending && turn.value === undefined && !turn.finished) {
        turn.value = state.value;
        turn.finished = state.finished;
      }
      turn.drives = Math.max(turn.drives, state.drives);
    });
    return turn.loaded;
  }

  private async read(turn: Turn, signal: AbortSignal): Promise<State> {
    const missing = (error: unknown) => {
      for (let value = error, depth = 0; value && typeof value === 'object' && depth < 5; value = (value as { cause?: unknown }).cause, depth++) {
        const entry = value as { code?: unknown; status?: unknown; statusCode?: unknown; name?: unknown };
        if (entry.code === 'ENOENT' || entry.status === 404 || entry.statusCode === 404 || entry.name === 'StreamNotFoundError') return true;
      }
      return false;
    };
    let tail: number;
    try { tail = (await this.world.streams.getInfo(turn.runId, turn.stream)).tailIndex; }
    catch (error) { if (missing(error)) return { drives: 0, finished: false }; throw error; }
    signal.throwIfAborted();
    if (tail < 0) return { drives: 0, finished: false };
    const stream = await this.world.streams.get(turn.runId, turn.stream, tail);
    const reader = stream.getReader();
    const cancel = () => { void reader.cancel().catch(() => {}); };
    signal.addEventListener('abort', cancel, { once: true });
    let text = '';
    let bytes = 0;
    const decoder = new TextDecoder();
    try {
      while (!text.includes('\n')) {
        signal.throwIfAborted();
        const next = await reader.read();
        if (next.done) break;
        bytes += next.value.byteLength;
        if (bytes > READ_LIMIT) throw new Error('Agent Runs checkpoint exceeds read limit');
        text += decoder.decode(next.value, { stream: true });
      }
    } finally { signal.removeEventListener('abort', cancel); cancel(); }
    signal.throwIfAborted();
    const line = decodeLine(text.split('\n')[0]!) as Partial<Line>;
    if (line?.version === 1 && typeof line.drive === 'number') return { value: line.v ?? undefined, drives: line.drive, finished: line.v === null };

    // Earlier previews wrote separate snapshot and drive lines. Read that history once, strictly.
    let value: TraceSnapshot | undefined;
    let drives = 0;
    let finished = false;
    let cursor: string | undefined;
    let buffer = '';
    bytes = 0;
    const legacyDecoder = new TextDecoder();
    for (;;) {
      signal.throwIfAborted();
      const page = await this.world.streams.getChunks(turn.runId, turn.stream, { ...(cursor ? { cursor } : {}) });
      for (const chunk of page.data) {
        bytes += chunk.data.byteLength;
        if (bytes > READ_LIMIT) throw new Error('Agent Runs checkpoint history exceeds read limit');
        buffer += legacyDecoder.decode(chunk.data, { stream: true });
      }
      if (!page.hasMore || !page.cursor) break;
      cursor = page.cursor;
    }
    for (const text of buffer.split('\n').filter(Boolean)) {
      const entry = decodeLine(text) as { v?: TraceSnapshot | null; drive?: number };
      if (typeof entry.drive === 'number') drives = Math.max(drives, entry.drive);
      if ('v' in entry) { value = entry.v ?? undefined; finished = entry.v === null; }
    }
    return { value, drives, finished };
  }

  private put(turn: Turn, value: TraceSnapshot | undefined, finished: boolean): Promise<void> {
    const line: Line = { version: 1, drive: turn.drives, ...(finished ? { v: null } : value === undefined ? {} : { v: value }) };
    return this.bounded(turn, () => this.world.streams.write(turn.runId, turn.stream, new TextEncoder().encode(`${encodeLine(line)}\n`)));
  }

  private write(key: string, value: TraceSnapshot | null): void {
    const turn = this.turn(key);
    if (!turn || turn.unavailable) return;
    turn.pending = { value };
    turn.value = value ?? undefined;
    turn.finished = value === null;
    if (turn.scheduled) return;
    turn.scheduled = true;
    turn.storing++;
    turn.writing = turn.writing.then(async () => {
      turn.scheduled = false;
      const pending = turn.pending;
      turn.pending = undefined;
      if (!pending || turn.unavailable) return;
      await this.load(turn);
      await this.put(turn, pending.value ?? undefined, pending.value === null);
    }).catch(() => {}).finally(() => { turn.storing--; });
  }
}

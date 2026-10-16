import { createHash } from 'node:crypto';
import type { World } from '@workflow/world';
import type { TraceCheckpointer, TraceSnapshot } from '@vercel/agent-runs';
import { decodeLine, encodeLine } from 'harness-world/streams';

/** One line of a turn's checkpoint stream: the turn's latest snapshot (null once it finished), or a drive that worked on it. */
type Line = { readonly v: TraceSnapshot | null } | { readonly drive: number };

interface Turn {
  readonly runId: string;
  readonly stream: string;
  /** Read from the stream (or written here since). Undefined until read. */
  loaded?: Promise<{ value: TraceSnapshot | undefined; drives: number; finished: boolean }>;
  value?: TraceSnapshot;
  /** Its last checkpoint is the deletion the library writes when the turn and its tool calls are done. */
  finished: boolean;
  drives: number;
  /** The newest value not yet written. */
  pending?: { value: TraceSnapshot | null };
  /** Writes of it being stored. */
  storing: number;
}

/**
 * The library's turn keys: `turn:<runId>:<turnId>`, URI-encoded. Each
 * turn's checkpoints are lines of a stream of its session's run, so they
 * live and expire with the session, and any process can read them.
 */
function parseKey(key: string): { runId: string; turnId: string } | undefined {
  const [kind, runId, turnId] = key.split(':');
  if (kind !== 'turn' || runId === undefined || turnId === undefined) return undefined;
  try {
    return { runId: decodeURIComponent(runId), turnId: decodeURIComponent(turnId) };
  } catch {
    return undefined;
  }
}

/**
 * The longest one checkpoint read or write holds a drive. Past it, a read
 * leaves the turn untraced in this drive, and a write leaves the turn's
 * older checkpoint, which the turn continues from with the same span ids.
 */
export const TRACE_IO_MS = 5_000;

export function checkpointStreamName(runId: string, turnId: string): string {
  return `${runId}_agent-runs_${createHash('sha256').update(turnId).digest('hex').slice(0, 24)}`;
}

/**
 * Durable checkpoints for one World's turns, in its streams. Reads come from
 * memory after a turn's first read in this process; writes return at once
 * and are stored in the background, newest value per turn only, in order
 * (`flush` waits for them). A process that dies before its last write leaves
 * an older checkpoint: the turn continues from it, with the same span ids.
 */
export class TraceStreams {
  private readonly turns = new Map<string, Turn>();
  private writing: Promise<void> = Promise.resolve();
  private scheduled = false;

  constructor(
    private readonly world: World,
    private readonly onError: (error: unknown) => void
  ) {}

  readonly checkpointer: TraceCheckpointer = {
    get: async (key) => {
      const turn = this.turn(key);
      if (!turn) return undefined;
      await this.load(turn);
      return turn.pending ? (turn.pending.value ?? undefined) : turn.value;
    },
    set: (key, value) => this.write(key, value),
    delete: (key) => this.write(key, null),
  };

  /**
   * A drive of `runId` (or a turn of it) starts: what this process remembers
   * of its turns may be older than another process's writes. A turn this
   * process is still writing is kept: its stream does not have that yet.
   */
  forget(runId: string, turnId?: string): void {
    const only = turnId === undefined ? undefined : `turn:${encodeURIComponent(runId)}:${encodeURIComponent(turnId)}`;
    for (const [key, turn] of this.turns) {
      if (turn.runId === runId && (only === undefined || key === only) && !turn.pending && turn.storing === 0) this.turns.delete(key);
    }
  }

  /**
   * The next drive's number for a turn, recorded before it is returned, so
   * two drives of a turn never get the same one (a drive that died without
   * writing anything else still took its number).
   */
  async nextDrive(runId: string, turnId: string): Promise<number> {
    const turn = this.turn(`turn:${encodeURIComponent(runId)}:${encodeURIComponent(turnId)}`)!;
    await this.load(turn);
    const drive = ++turn.drives;
    turn.storing++;
    try {
      await within(this.world.streams.write(runId, turn.stream, new TextEncoder().encode(`${encodeLine({ drive } satisfies Line)}\n`)), 'recording a drive');
    } finally {
      turn.storing--;
    }
    return drive;
  }

  /** Whether the turn was traced to its end (by any process): it must not be started again, or it would be traced twice. */
  async finished(runId: string, turnId: string): Promise<boolean> {
    const turn = this.turn(`turn:${encodeURIComponent(runId)}:${encodeURIComponent(turnId)}`)!;
    await this.load(turn);
    return turn.finished;
  }

  /** Wait until every write so far is stored, for at most `TRACE_IO_MS`: a drive never waits longer at its end. */
  async flush(): Promise<void> {
    const stored = (async () => {
      for (;;) {
        const writing = this.writing;
        await writing;
        if (writing === this.writing) return;
      }
    })();
    await within(stored, 'storing trace checkpoints').catch(this.onError);
  }

  private turn(key: string): Turn | undefined {
    let turn = this.turns.get(key);
    if (turn) return turn;
    const parsed = parseKey(key);
    if (!parsed) return undefined;
    turn = { runId: parsed.runId, stream: checkpointStreamName(parsed.runId, parsed.turnId), drives: 0, finished: false, storing: 0 };
    this.turns.set(key, turn);
    return turn;
  }

  /**
   * The turn's stream, read once. A read the World could not answer (it
   * failed for a while, or took too long) rejects: the turn goes untraced in
   * this drive rather than start again from an empty tree. `forget` lets a
   * later drive read again.
   */
  private async load(turn: Turn): Promise<void> {
    turn.loaded ??= within(readLines(this.world, turn.runId, turn.stream), 'reading a trace checkpoint').then(
      (lines) => {
        let value: TraceSnapshot | undefined;
        let drives = 0;
        let finished = false;
        for (const line of lines as Line[]) {
          if (line && typeof line === 'object' && 'drive' in line && typeof line.drive === 'number') drives = Math.max(drives, line.drive);
          else if (line && typeof line === 'object' && 'v' in line) {
            value = line.v ?? undefined;
            finished = line.v === null;
          }
        }
        return { value, drives, finished };
      },
      (error: unknown) => {
        this.onError(error);
        throw error;
      }
    );
    const { value, drives, finished } = await turn.loaded;
    if (!turn.pending && turn.value === undefined && !turn.finished) {
      turn.value = value;
      turn.finished = finished;
    }
    turn.drives = Math.max(turn.drives, drives);
  }

  private write(key: string, value: TraceSnapshot | null): void {
    const turn = this.turn(key);
    if (!turn) return;
    turn.pending = { value };
    turn.value = value ?? undefined;
    turn.finished = value === null;
    if (this.scheduled) return;
    this.scheduled = true;
    this.writing = this.writing.then(async () => {
      this.scheduled = false;
      for (const next of this.turns.values()) {
        const pending = next.pending;
        if (!pending) continue;
        next.pending = undefined;
        next.storing++;
        try {
          await within(this.world.streams.write(next.runId, next.stream, new TextEncoder().encode(`${encodeLine({ v: pending.value } satisfies Line)}\n`)), 'storing a trace checkpoint');
        } catch (error) {
          this.onError(error);
        } finally {
          next.storing--;
        }
      }
    });
  }
}

/** `work`, or a rejection once it took `TRACE_IO_MS` (it may still finish later). */
function within<T>(work: Promise<T>, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const late = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`agent-runs: ${what} took over ${TRACE_IO_MS} ms`)), TRACE_IO_MS);
    timer.unref?.();
  });
  return Promise.race([work, late]).finally(() => clearTimeout(timer));
}

/**
 * A stream's lines. A stream the World has no such thing as, or one that
 * expired, has none. A failure that says nothing about the stream (the
 * World's server, throttling, the network) rejects, rather than read as an
 * empty stream.
 */
async function readLines(world: World, runId: string, name: string): Promise<unknown[]> {
  const decoder = new TextDecoder();
  let buffer = '';
  let cursor: string | undefined;
  for (;;) {
    let page: Awaited<ReturnType<World['streams']['getChunks']>>;
    try {
      page = await world.streams.getChunks(runId, name, cursor ? { cursor } : {});
    } catch (error) {
      if (transient(error)) throw error;
      return [];
    }
    for (const chunk of page.data) buffer += decoder.decode(chunk.data, { stream: true });
    if (!page.hasMore || !page.cursor) break;
    cursor = page.cursor;
  }
  buffer += decoder.decode();
  return buffer.split('\n').filter(Boolean).map(decodeLine);
}

/** A World or network failure (its cause's, too): a 5xx, 425 or 429 answer, or a connection that failed. */
function transient(error: unknown): boolean {
  for (let e = error as { status?: unknown; code?: unknown; name?: unknown; cause?: unknown } | null | undefined, depth = 0; e && depth < 3; e = e.cause as typeof e, depth++) {
    if (typeof e.status === 'number') return e.status >= 500 || e.status === 425 || e.status === 429;
    if (e.name === 'AbortError' || e.name === 'TimeoutError' || e.code === 'TRANSPORT') return true;
    if (typeof e.code === 'string' && /^(ECONNRESET|ECONNREFUSED|ETIMEDOUT|EPIPE|EAI_AGAIN|UND_ERR_\w+)$/.test(e.code)) return true;
  }
  return false;
}

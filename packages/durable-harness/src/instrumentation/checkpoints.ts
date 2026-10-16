import { createHash } from 'node:crypto';
import type { World } from '@workflow/world';
import type { TraceCheckpointer, TraceSnapshot } from '@vercel/agent-runs';
import { encodeLine, readJsonLines } from 'harness-world/streams';

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
      await this.world.streams.write(runId, turn.stream, new TextEncoder().encode(`${encodeLine({ drive } satisfies Line)}\n`));
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

  /** Wait until every write so far is stored. */
  async flush(): Promise<void> {
    for (;;) {
      const writing = this.writing;
      await writing;
      if (writing === this.writing) return;
    }
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

  private async load(turn: Turn): Promise<void> {
    turn.loaded ??= readJsonLines(this.world, turn.runId, turn.stream).then(
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
        return { value: undefined, drives: 0, finished: false };
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
          await this.world.streams.write(next.runId, next.stream, new TextEncoder().encode(`${encodeLine({ v: pending.value } satisfies Line)}\n`));
        } catch (error) {
          this.onError(error);
        } finally {
          next.storing--;
        }
      }
    });
  }
}

import type { TraceSnapshot } from '@vercel/agent-runs';
import type { World } from '@workflow/world';
import { encodeLine } from 'harness-world/streams';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { checkpointStreamName, TraceStreams } from '../src/instrumentation/checkpoints.ts';

afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });

function memoryWorld() {
  const chunks = new Map<string, Uint8Array[]>();
  const reads: { name: string; start: number }[] = [];
  const streams = {
    async getInfo(_runId: string, name: string) { return { tailIndex: (chunks.get(name)?.length ?? 0) - 1, done: false }; },
    async get(_runId: string, name: string, start = 0) {
      reads.push({ name, start });
      return new ReadableStream<Uint8Array>({ start(controller) { for (const chunk of (chunks.get(name) ?? []).slice(start)) controller.enqueue(chunk); controller.close(); } });
    },
    async getChunks(_runId: string, name: string) { return { data: (chunks.get(name) ?? []).map((data, index) => ({ data, index })), cursor: null, hasMore: false, done: false }; },
    async write(_runId: string, name: string, data: Uint8Array) { chunks.set(name, [...(chunks.get(name) ?? []), data]); },
  };
  return { world: { streams } as unknown as World, streams, chunks, reads };
}
const snapshot = (value: string) => ({ value }) as unknown as TraceSnapshot;

describe('Agent Runs checkpoint storage', () => {
  it.each(['snapshot', 'delete', 'drive', 'sequence'] as const)('quarantines a timed-out %s write until the actual World write settles', async kind => {
    vi.useFakeTimers();
    const { world, streams, chunks } = memoryWorld();
    const put = streams.write.bind(streams);
    let release!: () => void;
    let writing = false;
    vi.spyOn(streams, 'write').mockImplementation(async (runId, name, data) => {
      if (runId === 'slow' && !writing) {
        writing = true;
        await new Promise<void>(resolve => { release = resolve; });
      }
      await put(runId, name, data);
    });
    const writer = new TraceStreams(world, () => {}, 10);
    let failed: Promise<unknown> | undefined;
    if (kind === 'snapshot') writer.checkpointer.set('turn:slow:turn', snapshot('old'));
    else if (kind === 'delete') writer.checkpointer.delete('turn:slow:turn');
    else failed = expect(kind === 'drive' ? writer.nextDrive('slow', 'turn') : writer.nextSequence('slow')).rejects.toThrow('timed out');
    const flushing = writer.flush('slow');
    await vi.advanceTimersByTimeAsync(11);
    await failed;
    await flushing;
    writer.forget('slow');
    // Cache pressure and finished-entry pruning must not drop the outstanding write either.
    for (let i = 0; i < 130; i++) await writer.checkpointer.get(`turn:other:${i}`);
    await writer.flush();
    await expect(kind === 'sequence' ? writer.nextSequence('slow') : writer.checkpointer.get('turn:slow:turn')).rejects.toThrow('timed out');
    if (kind !== 'sequence') writer.checkpointer.set('turn:slow:turn', snapshot('must not race'));
    await writer.flush('slow');
    expect([...chunks.keys()].filter(name => name.startsWith('slow_'))).toEqual([]);
    release();
    await vi.advanceTimersByTimeAsync(0);
    writer.forget('slow');
    if (kind === 'sequence') expect(await writer.nextSequence('slow')).toBe(1);
    else {
      expect(await writer.checkpointer.get('turn:slow:turn')).toEqual(kind === 'snapshot' ? snapshot('old') : undefined);
      writer.checkpointer.set('turn:slow:turn', snapshot('latest'));
      await writer.flush('slow');
      const recovered = new TraceStreams(world, () => {});
      expect(await recovered.checkpointer.get('turn:slow:turn')).toEqual(snapshot('latest'));
    }
  });

  it('keeps turn sequence numbers across workers without sharing a turn key', async () => {
    const { world } = memoryWorld();
    const first = new TraceStreams(world, () => {});
    expect(await first.nextSequence('run')).toBe(0);
    expect(await first.nextSequence('run')).toBe(1);
    const second = new TraceStreams(world, () => {});
    expect(await second.nextSequence('run')).toBe(2);
    expect(await first.nextSequence('run')).toBe(3);
    expect(await first.checkpointer.get('turn:run:sequence')).toBeUndefined();
  });

  it('recovers the latest checkpoint and drive number from only the last chunk', async () => {
    const { world, streams, chunks, reads } = memoryWorld();
    const writer = new TraceStreams(world, () => {});
    writer.checkpointer.set('turn:run:turn', snapshot('first'));
    await writer.flush('run');
    expect(await writer.nextDrive('run', 'turn')).toBe(1);
    writer.checkpointer.set('turn:run:turn', snapshot('latest'));
    await writer.flush('run');
    const history = vi.spyOn(streams, 'getChunks');
    const reader = new TraceStreams(world, () => {});
    expect(await reader.checkpointer.get('turn:run:turn')).toEqual(snapshot('latest'));
    const name = checkpointStreamName('run', 'turn');
    expect(reads.at(-1)).toEqual({ name, start: chunks.get(name)!.length - 1 });
    expect(history).not.toHaveBeenCalled();
    expect(await reader.nextDrive('run', 'turn')).toBe(2);
  });

  it('continues existing separate snapshot/drive records and writes self-contained records afterward', async () => {
    const { world, streams } = memoryWorld();
    const name = checkpointStreamName('run', 'turn');
    for (const line of [{ v: snapshot('existing') }, { drive: 4 }]) await streams.write('run', name, new TextEncoder().encode(`${encodeLine(line)}\n`));
    const reader = new TraceStreams(world, () => {});
    expect(await reader.checkpointer.get('turn:run:turn')).toEqual(snapshot('existing'));
    expect(await reader.nextDrive('run', 'turn')).toBe(5);
    reader.forget('run');
    const history = vi.spyOn(streams, 'getChunks');
    expect(await reader.checkpointer.get('turn:run:turn')).toEqual(snapshot('existing'));
    expect(history).not.toHaveBeenCalled();
  });

  it('distinguishes an absent checkpoint from a failed read and never overwrites the latter', async () => {
    const { world, streams, chunks } = memoryWorld();
    const reader = new TraceStreams(world, () => {});
    expect(await reader.checkpointer.get('turn:missing:turn')).toBeUndefined();
    vi.spyOn(streams, 'getInfo').mockRejectedValue(new Error('stream read unavailable'));
    await expect(reader.checkpointer.get('turn:unavailable:turn')).rejects.toThrow('stream read unavailable');
    reader.checkpointer.set('turn:unavailable:turn', snapshot('must not overwrite'));
    await reader.flush('unavailable');
    expect(chunks.has(checkpointStreamName('unavailable', 'turn'))).toBe(false);
  });

  it('does not treat a failed legacy history read as an empty checkpoint', async () => {
    const { world, streams } = memoryWorld();
    await streams.write('run', checkpointStreamName('run', 'turn'), new TextEncoder().encode(`${encodeLine({ drive: 1 })}\n`));
    vi.spyOn(streams, 'getChunks').mockRejectedValue(new Error('history unavailable'));
    const reader = new TraceStreams(world, () => {});
    await expect(reader.checkpointer.get('turn:run:turn')).rejects.toThrow('history unavailable');
  });

  it('bounds a stuck read by the remaining drive budget', async () => {
    vi.useFakeTimers();
    const { world, streams } = memoryWorld();
    vi.spyOn(streams, 'getInfo').mockImplementation(() => new Promise(() => {}));
    const reader = new TraceStreams(world, () => {}, 1000);
    reader.setDeadline('run', Date.now() + 10);
    const failed = expect(reader.checkpointer.get('turn:run:turn')).rejects.toThrow('timed out');
    await vi.advanceTimersByTimeAsync(11);
    await failed;
  });

  it('flushes one session without waiting for another session or an endless writer', async () => {
    vi.useFakeTimers();
    const { world, streams } = memoryWorld();
    const put = streams.write.bind(streams);
    vi.spyOn(streams, 'write').mockImplementation((runId, name, data) => runId === 'slow' ? new Promise(() => {}) : put(runId, name, data));
    const writer = new TraceStreams(world, () => { throw new Error('diagnostics unavailable'); }, 25);
    writer.checkpointer.set('turn:slow:turn', snapshot('slow'));
    writer.checkpointer.set('turn:fast:turn', snapshot('fast'));
    let finished = false;
    const fast = writer.flush('fast').then(() => { finished = true; });
    await vi.advanceTimersByTimeAsync(0);
    expect(finished).toBe(true);
    await fast;
    const slow = writer.flush('slow');
    await vi.advanceTimersByTimeAsync(26);
    await slow;
  });

  it('reloads drive numbers after idle checkpoint cache entries are evicted', async () => {
    const { world, reads } = memoryWorld();
    const writer = new TraceStreams(world, () => {});
    for (let i = 0; i < 3; i++) await writer.nextDrive('old', 'turn');
    for (let i = 0; i < 150; i++) {
      writer.checkpointer.set(`turn:run${i}:turn`, snapshot('value'));
      await writer.flush(`run${i}`);
    }
    expect(await writer.nextDrive('old', 'turn')).toBe(4);
    expect(reads.some((read) => read.name === checkpointStreamName('old', 'turn'))).toBe(true);
  });
});

import { describe, expect, it } from 'vitest';
import { countedTurns, foldItems, type Item } from './Compare.tsx';
import type { TurnView } from './compare-model.ts';

describe('compare foldItems', () => {
  it('joins text deltas, tracks tools, ignores marks', () => {
    let items: Item[] = [{ kind: 'user', text: 'hi' }];
    for (const e of [
      { type: 'text', delta: 'He' },
      { type: 'text', delta: 'llo' },
      { type: 'mark', name: 'ack', lagMs: 3 },
      { type: 'tool_start', name: 'bash' },
      { type: 'tool_end', name: 'bash', isError: true },
      { type: 'text', delta: 'done' },
    ])
      items = foldItems(items, e);
    expect(items).toEqual([
      { kind: 'user', text: 'hi' },
      { kind: 'text', text: 'Hello' },
      { kind: 'tool', name: 'bash', done: true, error: true },
      { kind: 'text', text: 'done' },
    ]);
  });
  it('drops the cut-off reply on restart, and keeps text the log kept', () => {
    const fold = (events: object[]) => events.reduce<Item[]>((items, e) => foldItems(items, e), [{ kind: 'user', text: 'hi' }]);
    const before = [{ type: 'text', delta: 'Sending it.' }, { type: 'tool_start', name: 'mail' }, { type: 'tool_end', name: 'mail' }, { type: 'text', delta: 'The email went' }];
    expect(fold([...before, { type: 'restart', text: 'Sending it.', parts: ['Sending it.'] }, { type: 'text', delta: 'Done.' }])).toEqual([
      { kind: 'user', text: 'hi' },
      { kind: 'text', text: 'Sending it.' },
      { kind: 'tool', name: 'mail', done: true, error: false },
      { kind: 'text', text: 'Done.' },
    ]);
    // A crash mid-tool: nothing streamed was cut off.
    const midTool = fold([{ type: 'text', delta: 'Sending it.' }, { type: 'tool_start', name: 'mail' }]);
    expect(foldItems(midTool, { type: 'restart', text: 'Sending it.', parts: ['Sending it.'] })).toBe(midTool);
    // A reply the log kept whole stays.
    const kept = fold([{ type: 'text', delta: 'All done.' }]);
    expect(foldItems(kept, { type: 'restart', text: 'All done.', parts: ['All done.'] })).toBe(kept);
    // A cut-off reply that repeats text the log kept before the tool still goes: the count, not the words, tells them apart.
    const repeat = fold([{ type: 'text', delta: 'Done.' }, { type: 'tool_start', name: 'mail' }, { type: 'tool_end', name: 'mail' }, { type: 'text', delta: 'Done.' }]);
    expect(foldItems(repeat, { type: 'restart', text: 'Done.', parts: ['Done.'] })).toEqual(repeat.slice(0, -1));
  });
  it('returns the same array for events that show nothing', () => {
    const items: Item[] = [];
    expect(foldItems(items, { type: 'mark', name: 'idle' })).toBe(items);
    expect(foldItems(items, null)).toBe(items);
  });
});

describe('compare summary turns', () => {
  const turn = (extra: Partial<TurnView> & { flagged?: boolean }): TurnView => {
    const { flagged, ...rest } = extra;
    return { sentAt: 0, segments: [], tools: [], turn: 1000, ...(flagged !== undefined ? { breakdown: { calls: [], tools: [], flags: {}, flagged } } : {}), ...rest };
  };
  const vs = [turn({ warmup: true }), turn({ flagged: true }), turn({ flagged: false }), turn({ turn: undefined })];
  it('counts every finished turn but the warm-up, flagged ones included', () => {
    expect(countedTurns(vs, false)).toEqual([vs[1], vs[2]]);
  });
  it('leaves flagged turns out only when asked', () => {
    expect(countedTurns(vs, true)).toEqual([vs[2]]);
  });
});

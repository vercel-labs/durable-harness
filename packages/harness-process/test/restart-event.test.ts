import { describe, expect, it } from 'vitest';
import type { OpenTurn } from '../src/records.ts';
import { restartEvent } from '../src/recovery.ts';

const entry = (data: unknown, seq: number) => ({ seq, at: seq, data }) as OpenTurn['entries'][number];

describe('restartEvent', () => {
  it("carries the turn's assistant text the log kept (finished steps, the text before a tool call), never a recovery note", () => {
    const turn: OpenTurn = {
      since: -1,
      placed: ['s1'],
      calls: [],
      entries: [
        entry({ type: 'message', role: 'assistant', text: 'Checking the file.', toolCalls: [{ id: 't1', name: 'read' }] }, 1),
        entry({ type: 'message', role: 'user', text: '[Recovery] Your previous turn was interrupted.', synthetic: 'recovery' }, 2),
        entry({ type: 'message', role: 'assistant', text: '', toolCalls: [{ id: 't2', name: 'bash' }] }, 3),
        entry({ type: 'message', role: 'assistant', text: 'It has 3 lines.' }, 4),
      ],
    };
    expect(restartEvent(turn)).toEqual({ type: 'restart', text: 'Checking the file.\n\nIt has 3 lines.', parts: ['Checking the file.', 'It has 3 lines.'] });
    expect(restartEvent({ ...turn, entries: [] })).toEqual({ type: 'restart', text: '', parts: [] });
  });
});

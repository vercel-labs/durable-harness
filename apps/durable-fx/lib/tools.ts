import { appendFile, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { FxAgentTool } from 'libfx';

/** Where `charge` records its charges: a stand-in for a payment API. */
export const LEDGER = process.env.LEDGER_FILE ?? join(tmpdir(), 'durable-fx-ledger.txt');

const pause = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener('abort', () => (clearTimeout(timer), resolve()), { once: true });
  });

/**
 * Two tools, one of each kind libfx tells apart:
 * - `wait` is idempotent: a call a crash cut off runs again when the turn continues.
 * - `charge` has effects: libfx stores the call before it runs and never runs
 *   it again by itself. After a crash the model hears that it may have partly
 *   run. A real tool passes `executionId` to its API as the idempotency key.
 */
export const tools: FxAgentTool[] = [
  {
    name: 'wait',
    description: 'Waits the given number of seconds.',
    inputSchema: { type: 'object', properties: { seconds: { type: 'number' } }, required: ['seconds'] },
    idempotent: true,
    async execute({ seconds }: { seconds: number }, { signal }) {
      await pause(Math.min(60, Math.max(0, seconds)) * 1000, signal);
      return `waited ${seconds} s`;
    },
  },
  {
    name: 'charge',
    description: 'Charges the user an amount in dollars, with a memo.',
    inputSchema: { type: 'object', properties: { amount: { type: 'number' }, memo: { type: 'string' } }, required: ['amount', 'memo'] },
    idempotent: false,
    async execute({ amount, memo }: { amount: number; memo: string }, { executionId, signal }) {
      await appendFile(LEDGER, `${executionId} $${amount.toFixed(2)} ${memo}\n`);
      await pause(3000, signal); // the payment provider confirming
      return `charged $${amount.toFixed(2)} (ref ${executionId.slice(-8)})`;
    },
  },
];

/** The ledger's lines, for the CLI. */
export const ledgerLines = async () => (await readFile(LEDGER, 'utf8').catch(() => '')).split('\n').filter(Boolean);

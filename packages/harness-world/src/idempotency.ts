import { sha256Hex } from './blobs.ts';

/**
 * The key `EffectIO.idempotencyKey()` gives the effect `key` (its memo key,
 * e.g. `tool:<call id>`) in session `runId`: `dh-` and the SHA-256 of both,
 * so it is the same for every run of that call, different for every other
 * call in any session, and made only of letters, digits and `-`.
 */
export async function effectIdempotencyKey(runId: string, key: string): Promise<string> {
  return `dh-${await sha256Hex(JSON.stringify(['durable-harness/idempotency', runId, key]))}`;
}

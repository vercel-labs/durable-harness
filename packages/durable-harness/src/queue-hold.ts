import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { QUEUE_HOLD_FILE, QUEUE_HOLD_GROUP_ENV } from './routing.ts';

/**
 * The consumer group a held wake is claimed for (harness-world's
 * `queueHoldConsumerGroup`): the group of the function that consumes
 * durable-harness's topic in this deployment, so a hold that lapses is
 * delivered where the message would have gone anyway. In order:
 *
 * - `DURABLE_HARNESS_QUEUE_HOLD_GROUP` in the environment;
 * - what the build recorded in `.durable-harness/queue-hold.json` (relative
 *   to the working directory, which is the project in a Vercel Function):
 *   `withDurableHarness` (Next.js) and `harnessFunctions()` (`vercel.ts`)
 *   write it and trace it into the functions that drive turns;
 * - nothing: no hold, and the chat route drives under its deadline with a
 *   delayed companion, as on a World without send-and-hold.
 *
 * An agent's own `queueHoldConsumerGroup` comes before all of these.
 */
export function queueHoldConsumerGroup(env: Record<string, string | undefined> = process.env, cwd: string = process.cwd()): string | undefined {
  const named = env[QUEUE_HOLD_GROUP_ENV]?.trim();
  if (named) return named;
  try {
    const recorded = (JSON.parse(readFileSync(join(cwd, QUEUE_HOLD_FILE), 'utf8')) as { consumerGroup?: unknown }).consumerGroup;
    return typeof recorded === 'string' && recorded ? recorded : undefined;
  } catch {
    return undefined;
  }
}

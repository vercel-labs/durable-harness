import type { HeldQueueMessage, NotHeldQueueMessage, QueueHandler, QueueNotHeldReason, QueueResult, World } from '@workflow/world';
import { QUEUE_NAME } from './queue.ts';

/**
 * Send-and-hold (docs/queue-send-and-hold.md): a wake the queue claims for
 * the process that sent it, before `world.queue()` returns. That process runs
 * it as a delivery of that message (`held.consume(handler)`), so the turn
 * starts in the request that submitted it with no queue hop, and the queue
 * stands behind it the way it stands behind any delivery: the lease is
 * renewed while the handler runs, and a lapsed lease redelivers the message.
 *
 * The World API is `@workflow/world`'s, experimental: `experimental_hold` on
 * `world.queue()`'s options and result, and `capabilities.experimental_queueHold`.
 * A World without the capability (world-local, world-postgres, the browser)
 * never sees the option, and keeps the deadline drive and its companion.
 */

/** What a held message's handler is passed: a queue delivery's metadata. */
export type HeldDeliveryMeta = Parameters<QueueHandler>[1];
/** `experimental_hold` with `status: 'held'`: a message the queue holds for its sender. */
export type HeldMessage = HeldQueueMessage;
/** `experimental_hold` with `status: 'not_held'`: the queue delivers the message. */
export type NotHeldMessage = NotHeldQueueMessage;
/** Why a held send did not come back held. The queue delivers the message in every case. */
export type NotHeldReason = QueueNotHeldReason;
/**
 * What `world.queue()` resolves with. `experimental_hold` is absent when no
 * hold was asked for or the World ignored it: the queue delivers the message
 * then too.
 */
export type HoldQueueResult = QueueResult;

/** Whether `world`'s queue can hold a message for its sender. */
export function worldHolds(world: World): boolean {
  return world.capabilities?.experimental_queueHold === true;
}

/**
 * What a consumer group may be named (`@workflow/world`'s
 * `QueueHoldOptions.consumerGroup`; world-vercel refuses anything else
 * before it sends).
 */
export const CONSUMER_GROUP = /^[A-Za-z0-9_-]+$/;

/** `HARNESS_QUEUE_HOLD=0`: never ask for a hold (see `DurableSessionsOptions.queueHoldConsumerGroup`). */
export function queueHoldDisabledByEnv(): boolean {
  return (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.HARNESS_QUEUE_HOLD === '0';
}

/**
 * Send a plain session wake (what `wake()` sends with no options) undelayed,
 * asking the queue to hold it for this process. The key makes a resend, or
 * the plain wake a failed send falls back to, the same message.
 * `consumerGroup` is the group that consumes the kernel's topic where this
 * runs: the hold is claimed for it, so a hold that lapses is delivered to
 * the Function that would have received the message anyway.
 */
export async function wakeHeld(world: World, runId: string, idempotencyKey: string, consumerGroup: string): Promise<HoldQueueResult> {
  return world.queue(QUEUE_NAME, { runId, input: {} }, { idempotencyKey, experimental_hold: { consumerGroup } });
}

import type { World } from '@workflow/world';
import { QUEUE_NAME } from './queue.ts';

/**
 * Send-and-hold (docs/queue-send-and-hold.md): a wake the queue claims for
 * the process that sent it, before `world.queue()` returns. That process runs
 * it as a delivery of that message (`held.consume(handler)`), so the turn
 * starts in the request that submitted it with no queue hop, and the queue
 * stands behind it the way it stands behind any delivery: the lease is
 * renewed while the handler runs, and a lapsed lease redelivers the message.
 *
 * The World API is experimental (`@workflow/world`'s `experimental_hold`,
 * `experimental_held`, `capabilities.experimental_queueHold`) and the World
 * types this package builds against do not carry it yet, so it is typed here,
 * structurally, and detected at run time. A World without the capability
 * never sees the option.
 */

/** What a held message's handler is passed: a queue delivery's metadata. */
export interface HeldDeliveryMeta {
  attempt: number;
  deliveryCount?: number;
  createdAt?: Date;
  queueName: string;
  messageId: string;
  requestId?: string;
  invocationStartedAt?: number;
}

/** `experimental_held`: a message the queue holds for its sender. */
export interface HeldMessage {
  readonly messageId: string;
  readonly leaseExpiresAt: Date;
  /** Run `handler` on the message here, as the World's queue handler runs a delivery of it. */
  consume(handler: (message: unknown, meta: HeldDeliveryMeta) => Promise<unknown>): Promise<void>;
  /** Give the message back unrun: the queue delivers it after `delaySeconds`. */
  release(delaySeconds?: number): Promise<void>;
}

/** Why a held send did not come back held. The queue delivers the message in every case. */
export type NotHeldReason = 'unsupported' | 'deferred' | 'lost' | 'coalesced' | 'concurrency_limited' | 'duplicate' | 'error';

/** What `world.queue()` resolves with on a World that holds. */
export interface HoldQueueResult {
  messageId: string | null;
  experimental_held?: HeldMessage;
  experimental_notHeld?: { reason: NotHeldReason | (string & {}) };
}

/** Whether `world`'s queue can hold a message for its sender. */
export function worldHolds(world: World): boolean {
  return (world.capabilities as { experimental_queueHold?: unknown } | undefined)?.experimental_queueHold === true;
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
  const queue = world.queue as (name: string, payload: unknown, opts: { idempotencyKey: string; experimental_hold: { consumerGroup: string; visibilityTimeoutSeconds?: number } }) => Promise<HoldQueueResult>;
  return queue.call(world, QUEUE_NAME, { runId, input: {} }, { idempotencyKey, experimental_hold: { consumerGroup } });
}

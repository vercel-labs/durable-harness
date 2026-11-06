/**
 * Where a World's queue delivers a session's turns: the one place in
 * durable-harness that says so.
 *
 * The kernel queues on a namespace of its own, `harness`
 * (`__harness_wkf_workflow_session`, owner checks, Pi's wakes, and a
 * restart's recovery on `__harness_wkf_workflow_<workflowName>`), never on
 * the Workflow SDK's prefix (`__wkf_workflow_`), so an app can use
 * durable-harness and the Workflow SDK together.
 *
 * - world-local and world-postgres deliver the prefix in process
 *   (`registerQueue`: `Queue.registerHandler`), or over HTTP to the flow
 *   route (`Queue.registerRoute`): a job takes the route its process
 *   registered, so a process that starts a World and makes no agents
 *   registers the route alone (`forwardQueue`).
 * - On Vercel, the Function at the flow route subscribes to the topic
 *   `__harness_wkf_workflow_*`.
 *
 * The flow route, in a Next.js app (`withDurableHarness` writes it):
 *
 * - without the Workflow SDK, at the Workflow SDK's path,
 *   `/.well-known/workflow/v1/flow` (`WORKFLOW_FLOW_ROUTE`), with its trigger
 *   in `config.json` next to it, which Vercel's Next.js builder applies to
 *   that route: no `vercel.json`;
 * - with the Workflow SDK (`withWorkflow`), which owns that path, at
 *   durable-harness's own, `/.well-known/durable-harness/v1/flow`
 *   (`FLOW_ROUTE`), and an fx agent's wakes at `/.well-known/durable-harness/v1/fx`
 *   (`FX_ROUTE`); on Vercel each takes a `functions` entry in `vercel.json` /
 *   `vercel.ts` (`npx durable-harness vercel-json --write`, or
 *   `harnessFunctions()` from `durable-harness/vercel`).
 *
 * An app on Vercel Functions mounts `flowHandler()` itself (`api/flow.ts`); a
 * server, at `FLOW_ROUTE` (or `DURABLE_HARNESS_FLOW_ROUTE`).
 */
export { QUEUE_NAMESPACE, QUEUE_PREFIX, QUEUE_TOPIC } from 'harness-world/queue';
import { QUEUE_TOPIC } from 'harness-world/queue';

/** durable-harness's own route, used with the Workflow SDK and by servers: the queue's consumer for every agent the app makes. */
export const FLOW_ROUTE = '/.well-known/durable-harness/v1/flow';

/** The fx route, durable-harness's own: libfx's wakes, for an fx agent (`createFxAgent`), in an app that also uses the Workflow SDK. */
export const FX_ROUTE = '/.well-known/durable-harness/v1/fx';

/**
 * The Workflow SDK's flow route: where `withDurableHarness` writes the flow
 * route in a Next.js app that does not use the Workflow SDK, so that Vercel's
 * Next.js builder gives it the trigger in `config.json` (no `vercel.json`). It
 * answers the Workflow tools' `HEAD ?__health` probe, as the Workflow SDK's does.
 */
export const WORKFLOW_FLOW_ROUTE = '/.well-known/workflow/v1/flow';

/** The routes `withDurableHarness` writes. */
export type GeneratedRoute = typeof FLOW_ROUTE | typeof FX_ROUTE | typeof WORKFLOW_FLOW_ROUTE;

/**
 * The flow Function's queue trigger: every kernel message. Without a
 * `consumer`: that is the form a `vercel.json` / `vercel.ts` function takes
 * (Vercel names the consumer after the function; a `consumer` there fails the
 * deploy).
 */
export function flowTrigger() {
  return { type: 'queue/v2beta' as const, topic: QUEUE_TOPIC, retryAfterSeconds: 5, initialDelaySeconds: 0 };
}

/** The flow Function's duration: as long as the plan allows (a turn that outlives the chat route's deadline goes on here). */
export const FLOW_MAX_DURATION = 'max' as const;

/**
 * libfx's queue: every fx agent (`createFxAgent` from `libfx`) queues its
 * session messages on this one topic (libfx's `queueName`), and
 * `agent.wakeHandler()` consumes it. A test keeps it equal to libfx's.
 */
export const LIBFX_QUEUE_PREFIX = '__libfx_wkf_workflow_';
export const LIBFX_TOPIC = `${LIBFX_QUEUE_PREFIX}session`;

/** The queue trigger for libfx's wakes, on the fx route's Function (or a wake route of the app's own). */
export function fxTrigger() {
  return { type: 'queue/v2beta' as const, topic: LIBFX_TOPIC };
}

/** What the flow route at the Workflow SDK's path consumes: durable-harness sessions' turns, libfx's wakes, or both. */
export interface FlowConsumers {
  harness: boolean;
  fx: boolean;
}

/**
 * The flow route's settings at the Workflow SDK's path, as `config.json` next
 * to it carries them (`workflows`): Vercel's Next.js builder applies these to
 * the route, as it does `@workflow/builders`' (with a `consumer`, which the
 * Next.js builder takes). With fx agents, a second trigger for libfx's topic:
 * the builder takes `workflows` as is (the one-trigger rule is
 * `vercel.json`'s), and a Function may carry several queue triggers; the
 * route tells the deliveries apart by topic.
 */
export function flowFunctionConfig(consumers: FlowConsumers = { harness: true, fx: false }) {
  return {
    version: '0',
    workflows: {
      maxDuration: FLOW_MAX_DURATION,
      experimentalTriggers: [
        ...(consumers.harness ? [{ ...flowTrigger(), consumer: CONFIG_JSON_CONSUMER }] : []),
        ...(consumers.fx ? [{ ...fxTrigger(), consumer: CONFIG_JSON_CONSUMER }] : []),
      ],
    },
  };
}

/**
 * The consumer group of a queue trigger in a Build Output `config.json` (the
 * flow route `withDurableHarness` writes at the Workflow SDK's path), as the
 * Workflow SDK's own routes name theirs.
 */
export const CONFIG_JSON_CONSUMER = 'default';

/**
 * The consumer group Vercel gives a `queue/v2beta` trigger of a function
 * configured in `vercel.json` / `vercel.ts` (which may not name one): the
 * function's `functions` key (a path or a glob, as written), prefixed with
 * `<service>~` in a project of several services, escaped to
 * `[A-Za-z0-9_-]`: `_` is `__`, `/` is `_S`, `.` is `_D`, letters, digits and
 * `-` stay, and any other character is `_` and its char code in uppercase
 * hex (at least two digits). `api/flow.ts` is `api_Sflow_Dts`.
 *
 * A copy of `@workflow/world-vercel`'s `vercelTriggerConsumerGroup` (and of
 * `@vercel/build-utils`' naming), until a released World carries it; a test
 * pins it.
 */
export function vercelTriggerConsumerGroup(functionPattern: string, serviceName?: string): string {
  const name = serviceName ? `${serviceName}~${functionPattern}` : functionPattern;
  let out = '';
  for (const c of name) {
    if (c === '_') out += '__';
    else if (c === '/') out += '_S';
    else if (c === '.') out += '_D';
    else if (/[A-Za-z0-9-]/.test(c)) out += c;
    else out += `_${c.codePointAt(0)!.toString(16).toUpperCase().padStart(2, '0')}`;
  }
  return out;
}

/**
 * Where a build records the consumer group of the function that consumes
 * durable-harness's topic, for the functions that send held wakes (the chat
 * routes) to read at run time: project-relative, traced into each of them.
 * See `queueHoldConsumerGroup`.
 */
export const QUEUE_HOLD_FILE = '.durable-harness/queue-hold.json';

/** Names the consumer group outright, over what the build recorded. */
export const QUEUE_HOLD_GROUP_ENV = 'DURABLE_HARNESS_QUEUE_HOLD_GROUP';

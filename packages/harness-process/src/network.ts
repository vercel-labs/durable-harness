/**
 * The session VM's network as a process harness reads it (`sandbox({ network })`).
 *
 * The session factory applies the policy itself, rules and all (header
 * transforms included), on every attach. A harness only needs what to derive
 * from it: whether provider-hosted tools and provider-side fetches may run
 * (the gateway handler's `network`), and which domains the CLI's own sandbox
 * lets commands reach (Claude Code). `networkSummary` reduces a policy to
 * that, its mode and the domains it names, never its rules, so a header
 * value never reaches a harness.
 */

/** A policy as a harness reads it: the mode, or the domains a custom policy names (`*` for every other domain). */
export type MachineNetwork = 'allow-all' | 'deny-all' | { readonly allow: readonly string[] };

/** The session a per-session network callback is called for. */
export interface MachineNetworkSession {
  /** The session (`runId`). */
  readonly sessionId: string;
}

/**
 * `sandbox({ network })` as a harness gets it from the descriptor,
 * structurally (this package doesn't load `@vercel/sandbox`): a shorthand, a
 * full `NetworkPolicy`, or a per-session callback the session factory calls on
 * each attach.
 */
export type MachineNetworkOption =
  | MachineNetwork
  | { readonly allow?: unknown; readonly subnets?: unknown }
  | ((session: MachineNetworkSession) => unknown);

/** The gateway handler's view of the network: provider-hosted tools are refused under `deny-all`, provider-side fetches allowed only under `allow-all`. */
export type GatewayNetwork = 'allow-all' | 'deny-all' | 'custom';

/**
 * A policy reduced to its mode and the domains it names (`MachineNetwork`).
 * `undefined` (the descriptor's default) is `allow-all`; a callback is
 * `undefined`, known only once an attach resolved it (`SessionMachine.network`).
 * A custom policy is never `allow-all`, whatever its rules say, and its
 * subnets are dropped (nothing a harness derives can say them): the summary
 * is never more open than the policy.
 */
export function networkSummary(network: MachineNetworkOption | undefined): MachineNetwork | undefined {
  if (network === undefined || network === 'allow-all') return 'allow-all';
  if (network === 'deny-all') return 'deny-all';
  if (typeof network === 'function') return undefined;
  if (typeof network !== 'object' || network === null) return 'deny-all';
  const allow = (network as { allow?: unknown }).allow;
  const domains = Array.isArray(allow) ? allow.filter((d): d is string => typeof d === 'string') : typeof allow === 'object' && allow !== null ? Object.keys(allow) : [];
  return Object.freeze({ allow: Object.freeze([...new Set(domains)]) });
}

/** The gateway handler's mode for a summary; one not known yet (a callback no attach resolved) is `deny-all`, the narrowest. */
export function gatewayNetwork(network: MachineNetwork | undefined): GatewayNetwork {
  if (network === 'allow-all' || network === 'deny-all') return network;
  return network === undefined ? 'deny-all' : 'custom';
}

import type { Finding, InvariantId, Phase } from './invariants.ts';
import type { WorldName } from './worlds.ts';

/**
 * Gaps the check knows about. Each is asserted to still be there (a strict
 * expected failure, `it.fails`): fixing one fails its test until its entry
 * here is deleted. `owner` says whose fix each one is.
 */
export interface KnownIssue {
  invariant: InvariantId;
  /** Scenario name(s), or `*`. */
  scenario: string | string[];
  world: WorldName | '*';
  phase?: Phase | '*';
  /** Session label (`basic`, `recovered`), or `*`. */
  label?: string | '*';
  /** The finding codes this covers. Findings of the same invariant with other codes still fail the run. */
  codes: string[];
  reason: string;
  /** Whose fix it is: this repo's, or the Workflow SDK's. */
  owner: 'ours' | 'workflow';
  /** Applies only when this holds (an environment the issue needs). */
  when?: () => boolean;
  /** A race: tolerated and reported when it shows, but not asserted to (no `it.fails`). */
  intermittent?: boolean;
}

export const KNOWN: KnownIssue[] = [
  {
    invariant: 'payloads',
    scenario: 'pi-world',
    world: '*',
    codes: ['string-payload'],
    reason: "pi-world journal entries are JSON text inside the devalue payload (devalue refuses Pi's __proto__ keys and lone surrogates), so the CLI shows a string",
    owner: 'ours',
  },
  {
    invariant: 'status',
    scenario: 'pi-world',
    world: '*',
    phase: 'closed',
    codes: ['no-close'],
    reason: 'pi-world has no close path: its runs stay running',
    owner: 'ours',
  },
];

export interface Cell {
  scenario: string;
  world: WorldName;
  label: string;
  phase: Phase;
}

const matches = (pattern: string | string[] | undefined, value: string) =>
  pattern === undefined || pattern === '*' || (Array.isArray(pattern) ? pattern.includes(value) : pattern === value);

/** The known issues that apply to one invariant of one session. */
export function knownFor(invariant: InvariantId, cell: Cell, known: KnownIssue[] = KNOWN): KnownIssue[] {
  return known.filter(
    (k) => (k.when?.() ?? true) && k.invariant === invariant && matches(k.scenario, cell.scenario) && matches(k.world, cell.world) && matches(k.phase, cell.phase) && matches(k.label, cell.label)
  );
}

/** Split findings into the ones known issues cover and the rest. */
export function partition(findings: Finding[], known: KnownIssue[]): { covered: Finding[]; unexpected: Finding[] } {
  const codes = new Set(known.flatMap((k) => k.codes));
  return { covered: findings.filter((f) => codes.has(f.code)), unexpected: findings.filter((f) => !codes.has(f.code)) };
}

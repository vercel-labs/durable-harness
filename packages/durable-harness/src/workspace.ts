/**
 * Where a model's hands go: the two environments every harness takes
 * (docs/workspaces.md is the design).
 *
 * The rule: your Function runs only code you wrote. A HostTool runs in the
 * Function (the model picks only its arguments); every shell command and
 * file path the model chooses runs in one of these instead:
 *
 * - `virtual()`: just-bash on a virtual filesystem, in the Function.
 *   Commands can't reach the Function's files, processes or environment.
 *   `durable: true` (the default) commits each tool call's file changes with
 *   its outcome in the session's log, so they follow the session to any
 *   machine; `durable: false` keeps them in memory and clears them at the
 *   end of every turn.
 * - `sandbox()`: a Vercel Sandbox VM per session, with a Drive per session
 *   at `/workspace`. Files on the Drive are durable but not transactional (a
 *   command a crash interrupted may have half-run), so mutating calls are
 *   replay `never`. The VM holds no secret and never writes to the World;
 *   the harness's driver stays in the Function and writes the log.
 *
 * Claude Code and Codex run only in a sandbox. At launch, Pi, pi-durable, fx
 * and OpenCode keep their core in the Function and send commands and files to
 * either environment (docs/workspaces.md, open question 2).
 *
 * ```ts
 * import { sandbox, virtual } from 'durable-harness'
 * export const agent = opencode({ model, workspace: virtual() })
 * export const coder = claudeCode({ model, workspace: sandbox(), builtins: true })
 * ```
 *
 * This module is the contract only: plain, frozen descriptors with validated
 * options. Nothing here imports just-bash or `@vercel/sandbox`, starts a VM
 * or reads the environment; each factory resolves a descriptor into its
 * implementation on a session's first invocation.
 *
 * Words: a "driver" is one invocation of the harness in a Function (the
 * kernel's drive); "the Drive" is the Vercel Sandbox storage volume.
 */

/** Marks the objects `virtual()` and `sandbox()` return (`isWorkspace`). Registered, so two copies of the package agree. */
const BRAND: unique symbol = Symbol.for('durable-harness.workspace') as never;

/** Either environment. */
export type Workspace = VirtualWorkspace | SandboxWorkspace;

/** Which environment a descriptor is. */
export type WorkspaceKind = Workspace['kind'];

/**
 * Where a UI reads a session's files from: the log (`hw.workspace`, or the
 * runtime's own store with the same manifest shape), the Drive manifest the
 * driver keeps in the log (`hw.sandbox-files`, best effort; the Drive is the
 * source of truth), or nowhere (`durable: false`: in memory only).
 */
export type FilesSource = 'log' | 'drive' | 'ephemeral';

/** Whether `value` came from `virtual()` or `sandbox()` (not a copy of one: copies lose the brand). */
export function isWorkspace(value: unknown): value is Workspace {
  return typeof value === 'object' && value !== null && (value as { [BRAND]?: unknown })[BRAND] === true;
}

// ---------------------------------------------------------------------------
// virtual()

export interface VirtualOptions {
  /**
   * `true`: each tool call's file changes commit with its outcome, in the
   * session's log (files over 4 KB in its blob stream): a call a crash
   * interrupted leaves nothing behind, and the files move with the session.
   * `false`: the same tools, in memory only, cleared at the end of every
   * turn, and sooner if the turn moves to another invocation or process;
   * the tool descriptions say so. Default `true`.
   */
  durable?: boolean;
  /**
   * The most bytes the session's virtual filesystem may hold. A write past it
   * is a tool error ("the workspace is full"), never a crash. A durable
   * workspace is also bounded by its log document (about 500 files).
   * Default 64 MiB, at most 256 MiB (the instance is shared).
   */
  maxBytes?: number;
  /** The largest single file. Default 8 MiB (at most `maxBytes`). */
  maxFileBytes?: number;
  /**
   * A command's wall-clock limit. Commands run in a worker thread leased to
   * this session alone, terminated at this deadline or when the invocation
   * ends, whichever comes first: one session's runaway command can't stall
   * the instance for the others. The call fails with a definite timeout, and
   * under `durable: true` its changes are rolled back. Default 60 s, at most
   * 5 minutes.
   */
  timeoutMs?: number;
}

/** What `virtual()` returns. */
export interface VirtualWorkspace {
  readonly kind: 'virtual';
  readonly durable: boolean;
  readonly maxBytes: number;
  readonly maxFileBytes: number;
  readonly timeoutMs: number;
  readonly [BRAND]: true;
}

/** `virtual()`'s defaults. */
export const VIRTUAL_DEFAULTS = Object.freeze({
  durable: true,
  maxBytes: 64 * 1024 * 1024,
  maxFileBytes: 8 * 1024 * 1024,
  timeoutMs: 60_000,
});

/** The most memory one session's virtual workspace may ask for on a shared instance. */
const VIRTUAL_MAX_BYTES = 256 * 1024 * 1024;
/** The longest one virtual command may run. */
const VIRTUAL_MAX_TIMEOUT_MS = 5 * 60_000;

const VIRTUAL_KEYS = ['durable', 'maxBytes', 'maxFileBytes', 'timeoutMs'] as const;

/** Options that are planned but not here yet, with what to say about them. */
const VIRTUAL_LATER: Record<string, string> = {
  store: "store: 'blob' (files in Vercel Blob, the tree's root hash in the log) is planned after launch; the files live in the log for now",
  machine: 'a machine for a virtual workspace is the demo\'s hybrid hands, not an option: give the agent sandbox() for a real machine',
};

/**
 * A virtual workspace: just-bash on a virtual filesystem at `/workspace`,
 * in the Function. No network, no real binaries (node, python, git), and no
 * way to reach the Function's files, processes or environment.
 *
 * `virtual(base, overrides)` derives one from another (a spread would lose
 * the brand).
 */
export function virtual(options?: VirtualOptions): VirtualWorkspace;
export function virtual(base: VirtualWorkspace, overrides?: VirtualOptions): VirtualWorkspace;
export function virtual(first: VirtualOptions | VirtualWorkspace = {}, overrides: VirtualOptions = {}): VirtualWorkspace {
  const opts = merged('virtual', first, overrides);
  checkKeys('virtual', opts, VIRTUAL_KEYS, VIRTUAL_LATER);
  const durable = opts.durable ?? VIRTUAL_DEFAULTS.durable;
  if (typeof durable !== 'boolean') throw invalid('virtual', 'durable', 'must be true or false', durable);
  const maxBytes = int('virtual', 'maxBytes', opts.maxBytes ?? VIRTUAL_DEFAULTS.maxBytes, 1024, VIRTUAL_MAX_BYTES);
  const maxFileBytes = int('virtual', 'maxFileBytes', opts.maxFileBytes ?? Math.min(VIRTUAL_DEFAULTS.maxFileBytes, maxBytes), 1, maxBytes);
  const timeoutMs = int('virtual', 'timeoutMs', opts.timeoutMs ?? VIRTUAL_DEFAULTS.timeoutMs, 1_000, VIRTUAL_MAX_TIMEOUT_MS);
  return brand({ kind: 'virtual', durable, maxBytes, maxFileBytes, timeoutMs } as const);
}

// ---------------------------------------------------------------------------
// sandbox()

/**
 * The VM's outbound network. The VM holds no credential for the AI Gateway,
 * the Vercel API or your app, so a call there from the VM fails either way;
 * where the policy allows it, the VM's network policy also answers 403 for
 * them. The VM's only way to a model is through the driver, which adds the
 * credential in your Function.
 *
 * - `allow-all` (default): everything else is open (npm, git, curl work).
 * - `deny-all`: nothing, DNS included. Claude Code and Codex still work:
 *   their model calls go through the driver, not the VM's network.
 * - `{ allow }`: only these domains (`*.example.com` for subdomains).
 *
 * Provider-hosted tools (web search or fetch run by the model provider) don't
 * use the VM's network; they are off unless your app turns them on.
 */
export type SandboxNetwork = 'allow-all' | 'deny-all' | { readonly allow: readonly string[] };

/**
 * Vercel credentials for a driver that runs outside Vercel (a self-hosted
 * server). They stay in the driver's process: they are never recorded on the
 * session and never sent to the VM. On Vercel, and on a laptop after
 * `vercel link`, leave them out (the Function's OIDC token is used).
 */
export interface SandboxCredentials {
  token: string;
  teamId: string;
  projectId: string;
}

/**
 * Spend caps for one session's sandbox, recorded in its log and enforced by
 * the driver: a model call past a cap is refused, and the turn ends with a
 * message saying which cap. `Infinity` turns a cap off.
 */
export interface SandboxBudget {
  /** Model spend (from the AI Gateway's usage) in one turn, in US dollars. Default `Infinity` (no cap): a cap that ends a long turn surprises more than it protects; set one for your app. */
  turnUsd?: number;
  /** Model spend over the session's life, in US dollars. Default `Infinity` (no cap). */
  sessionUsd?: number;
  /** VM time over the session's life, in seconds (the VM isn't extended past it). Default 4 hours. */
  vmSeconds?: number;
}

/**
 * What `agent.sessions.close` does to the session's sandbox, once the
 * session is closed. None of them boots a VM.
 *
 * - `stop` (default): stop the session's VM if it is running. The Drive and
 *   its files stay, for `deleteSessionSandbox` or `sandboxCleanup` later.
 * - `delete`: stop the VM, then delete the session's sandbox and its Drive
 *   (`deleteSessionSandbox`). Deleting the Drive deletes the session's files
 *   for good.
 * - `keep`: leave both alone: the VM runs until its idle timeout (`idleMs`),
 *   and the Drive stays.
 */
export type SandboxOnClose = 'stop' | 'delete' | 'keep';

export interface SandboxOptions {
  /**
   * The Vercel region of the session's VM and Drive (they must match, and a
   * Drive never moves). Default: the region of the Function that first
   * drives the session (`VERCEL_REGION`, when Vercel Sandbox offers it),
   * else `iad1`. Once the Drive exists its region wins: later invocations
   * adopt it from the Drive.
   */
  region?: string;
  /**
   * What the VM boots from: a VCR image, or a snapshot, carrying the relay
   * and the pinned harness binaries (built by durable-harness's recipe).
   * Default: a base snapshot per project that durable-harness builds on
   * first use for its pinned versions.
   */
  image?: string | { readonly snapshot: string };
  /** The VM's vCPUs (2 GiB of memory each). Default 2. */
  vcpus?: 1 | 2 | 4 | 8;
  /** See `SandboxNetwork`. Default `allow-all`. */
  network?: SandboxNetwork;
  /**
   * Plain configuration for the model's commands (and the CLI). The model
   * can read every value, and with network on it can send it out: no
   * secrets. Refused, best effort: names that look like credentials
   * (`*TOKEN*`, `*SECRET*`, `*PASSWORD*`, `*OIDC*`, `*CREDENTIAL*`, a `KEY`,
   * `AUTH`, `DSN`, `PAT` or `SK` segment), values that look like one (URL
   * userinfo, `sk-…`, `ghp_…`, `github_pat_…`, `xox?-…`, `AKIA…`, a JWT), and
   * names the harness sets itself (`PATH`, `HOME`, `ANTHROPIC_*`, `CLAUDE_*`,
   * `CODEX_*`, ...). When it attaches, the driver also refuses any value
   * equal to one of the Function's environment values of 16 characters or
   * more.
   */
  env?: Readonly<Record<string, string>>;
  /**
   * How long the VM stays up after an invocation, so the next turn attaches
   * warm. Default 5 minutes, at most 4 hours; clamped at run time to what the
   * plan allows a VM session (45 minutes on Hobby).
   */
  idleMs?: number;
  /**
   * The longest a single command may run in the VM. Default 10 minutes, at
   * most 1 hour, and always capped by the time left in the current
   * invocation: a command that would outlive it ends with a definite timeout
   * and is never rerun.
   */
  commandTimeoutMs?: number;
  /**
   * The Drive's size in bytes, a multiple of 4096 from 1 GiB to 16 TiB (the
   * platform's bounds for one Drive), fixed for the session's life. Storage
   * is billed by the bytes used, not this size, so this is what bounds one
   * session's storage spend. Default 10 GiB. A plan whose Drive quota
   * refuses the size ("must not exceed the Drive quota of N bytes"; Hobby
   * allows 1 GiB a Drive) gets a Drive of the platform's default size
   * instead, with a line in the log. An existing Drive keeps its own size:
   * later invocations adopt it from the Drive.
   */
  driveSize?: number;
  /** See `SandboxBudget`. */
  budget?: SandboxBudget;
  /** See `SandboxCredentials`. Default: the environment's. */
  credentials?: SandboxCredentials;
  /** See `SandboxOnClose`: what closing a session does to its sandbox. Default `stop`; `sessions.close(id, { sandbox })` overrides it for one session. */
  onClose?: SandboxOnClose;
}

/**
 * What `sandbox()` returns. `credentials` is held off the enumerable
 * properties, so logging or serializing a descriptor never prints a token.
 * Derive one from another with `sandbox(base, overrides)`: a spread drops
 * `credentials` and the brand.
 */
export interface SandboxWorkspace {
  readonly kind: 'sandbox';
  /** Undefined: decided on the session's first invocation (see `SandboxOptions.region`). */
  readonly region?: string;
  readonly image?: string | { readonly snapshot: string };
  readonly vcpus: 1 | 2 | 4 | 8;
  readonly network: SandboxNetwork;
  readonly env: Readonly<Record<string, string>>;
  readonly idleMs: number;
  readonly commandTimeoutMs: number;
  readonly driveSize: number;
  readonly budget: Readonly<Required<SandboxBudget>>;
  readonly onClose: SandboxOnClose;
  /** Not enumerable. */
  readonly credentials?: Readonly<SandboxCredentials>;
  readonly [BRAND]: true;
}

/** `sandbox()`'s defaults. */
export const SANDBOX_DEFAULTS = Object.freeze({
  vcpus: 2 as const,
  network: 'allow-all' as const,
  idleMs: 5 * 60_000,
  commandTimeoutMs: 10 * 60_000,
  driveSize: 10 * 1024 ** 3,
  budget: Object.freeze({ turnUsd: Infinity, sessionUsd: Infinity, vmSeconds: 4 * 60 * 60 }),
  onClose: 'stop' as const,
});

/**
 * What a session's log records about its sandbox (the `hw.sandbox`
 * document). The names are derived from the project and the session, and
 * the Drive itself carries its region and size, so the record exists to
 * notice change: a new `driveId` means the files were lost (the model is
 * told once), and new `versions` mean a CLI upgrade (forward only). It is a
 * versioned document, updated when either changes.
 */
export interface SandboxRecord {
  readonly version: 1;
  /** The session's named sandbox (`dh-<hash of project and session>`). Every VM session of it shares the name. */
  readonly sandboxName: string;
  readonly driveName: string;
  /** The Drive's id. A different id on a later invocation means the files were lost. */
  readonly driveId: string;
  /** Adopted from the Drive. */
  readonly region: string;
  /** Adopted from the Drive. */
  readonly driveSize: number;
  /** The image or snapshot the VM booted from, and the CLI versions it carried. */
  readonly image: string;
  readonly versions: Readonly<Record<string, string>>;
  /** VM time granted so far, in seconds, against `sandbox({ budget: { vmSeconds } })` (absent while that cap is off). */
  readonly vmSeconds?: number;
}

const SANDBOX_KEYS = ['region', 'image', 'vcpus', 'network', 'env', 'idleMs', 'commandTimeoutMs', 'driveSize', 'budget', 'credentials', 'onClose'] as const;

const SANDBOX_LATER: Record<string, string> = {
  persistent: 'session VMs are never persistent: the Drive holds the files and the log the session, so every boot is a fresh VM',
  drive: 'every sandbox has a Drive at /workspace',
  failoverRegions: 'failover regions are not supported with a Drive mounted',
  networkPolicy: 'use network (allow-all, deny-all or { allow })',
  timeout: 'use idleMs and commandTimeoutMs: the driver keeps the VM up while it runs',
};

const VCPUS = [1, 2, 4, 8] as const;
/** A Vercel region id (`iad1`, `sfo1`, ...). */
const REGION = /^[a-z]{3}\d$/;
const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
/** Words that mark a credential anywhere in a name (`GITHUBTOKEN`), and words that do as a whole `_` segment (`API_KEY`, not `MONKEY`; `X_AUTH`, not `GIT_AUTHOR_NAME`). */
const SECRET_WORD = /TOKEN|SECRET|PASSWORD|PASSWD|OIDC|CREDENTIAL|APIKEY|PRIVATE_?KEY/i;
const SECRET_SEGMENT = /^(?:KEY|KEYS|AUTH|PASS|COOKIE|BEARER|JWT|SIGNATURE|DSN|PAT|SK)$/i;
const isSecretName = (name: string) => SECRET_WORD.test(name) || name.split('_').some((segment) => SECRET_SEGMENT.test(segment));
/**
 * Value shapes that are credentials whatever the name says. Best effort: the
 * driver adds, at attach, a check against the Function's own environment
 * values. Exported for the driver.
 */
export const SECRET_VALUE_SHAPES: readonly (readonly [string, RegExp])[] = Object.freeze([
  ['a URL with a user or password', /^[a-z][a-z0-9+.-]*:\/\/[^/\s?#]*@/i],
  ['an OpenAI or Anthropic style key (sk-…)', /^sk-[A-Za-z0-9_-]{16,}/],
  ['a GitHub token', /^(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})/],
  ['a Slack token', /^xox[abposr]-/],
  ['an AWS access key id', /^(?:AKIA|ASIA)[A-Z0-9]{16}$/],
  ['a JWT', /^eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\./],
  ['a Vercel token', /^vc[pkri]_[A-Za-z0-9]{20,}/],
  ['a PEM private key', /-----BEGIN [A-Z ]*PRIVATE KEY-----/],
] as const);
/** Why `value` looks like a credential, or undefined. */
export function secretValueShape(value: string): string | undefined {
  for (const [what, shape] of SECRET_VALUE_SHAPES) if (shape.test(value)) return what;
  return undefined;
}
const RESERVED_NAME = /^(?:PATH|HOME|USER|SHELL|TMPDIR|LANG|TERM|PWD|XDG_[A-Z_]+|ANTHROPIC_[A-Z_]*|CLAUDE_[A-Z_]*|CODEX_[A-Z_]*|OPENAI_[A-Z_]*|OPENCODE[A-Z_]*|VERCEL_[A-Z_]*|AI_GATEWAY_[A-Z_]*|HARNESS_[A-Z_]*|NODE_OPTIONS|LD_[A-Z_]+|BASH_ENV|ENV)$/;
const DOMAIN = /^(?:\*\.)?(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63}$/;
const DRIVE_BLOCK = 4096;
const DRIVE_MIN = 1024 * 1024 * 1024;
const DRIVE_MAX = 16 * 1024 ** 4;
const IDLE_MAX_MS = 4 * 60 * 60_000;
const COMMAND_MAX_MS = 60 * 60_000;

/**
 * A sandbox: one named Vercel Sandbox per session, with a Drive per session
 * mounted at `/workspace`. The CLI (Claude Code, Codex) or the model's
 * commands (Pi, pi-durable, fx, OpenCode) run there as a user without sudo;
 * the driver reaches the VM over connections it opens, and makes the VM's
 * model calls itself.
 *
 * `sandbox(base, overrides)` derives one from another, keeping `credentials`.
 */
export function sandbox(options?: SandboxOptions): SandboxWorkspace;
export function sandbox(base: SandboxWorkspace, overrides?: SandboxOptions): SandboxWorkspace;
export function sandbox(first: SandboxOptions | SandboxWorkspace = {}, overrides: SandboxOptions = {}): SandboxWorkspace {
  const opts = merged('sandbox', first, overrides);
  checkKeys('sandbox', opts, SANDBOX_KEYS, SANDBOX_LATER);
  const region = opts.region;
  if (region !== undefined && (typeof region !== 'string' || !REGION.test(region))) throw invalid('sandbox', 'region', "must be a Vercel region id such as 'iad1'", region);
  const image = sandboxImage(opts.image);
  const vcpus = opts.vcpus ?? SANDBOX_DEFAULTS.vcpus;
  if (!(VCPUS as readonly unknown[]).includes(vcpus)) throw invalid('sandbox', 'vcpus', 'must be 1, 2, 4 or 8', vcpus);
  const network = sandboxNetwork(opts.network ?? SANDBOX_DEFAULTS.network);
  const env = checkSandboxEnvVars(opts.env ?? {});
  const idleMs = int('sandbox', 'idleMs', opts.idleMs ?? SANDBOX_DEFAULTS.idleMs, 0, IDLE_MAX_MS);
  const commandTimeoutMs = int('sandbox', 'commandTimeoutMs', opts.commandTimeoutMs ?? SANDBOX_DEFAULTS.commandTimeoutMs, 1_000, COMMAND_MAX_MS);
  const driveSize = int('sandbox', 'driveSize', opts.driveSize ?? SANDBOX_DEFAULTS.driveSize, DRIVE_MIN, DRIVE_MAX);
  if (driveSize % DRIVE_BLOCK !== 0) throw invalid('sandbox', 'driveSize', `must be a multiple of ${DRIVE_BLOCK}`, driveSize);
  const budget = sandboxBudget(opts.budget ?? {});
  const onClose = sandboxOnClose(opts.onClose ?? SANDBOX_DEFAULTS.onClose, 'sandbox', 'onClose');
  const descriptor: Record<string, unknown> = { kind: 'sandbox', vcpus, network, env, idleMs, commandTimeoutMs, driveSize, budget, onClose };
  if (region !== undefined) descriptor.region = region;
  if (image !== undefined) descriptor.image = image;
  if (opts.credentials !== undefined) {
    // Not enumerable: JSON.stringify, spreads and console.log skip it.
    Object.defineProperty(descriptor, 'credentials', { value: sandboxCredentials(opts.credentials), enumerable: false });
  }
  return brand(descriptor) as unknown as SandboxWorkspace;
}

/** `onClose` (or `sessions.close`'s `sandbox`), checked: `stop`, `delete` or `keep`. */
export function sandboxOnClose(value: unknown, fn: string, option: string): SandboxOnClose {
  if (value === 'stop' || value === 'delete' || value === 'keep') return value;
  throw invalid(fn, option, "must be 'stop', 'delete' or 'keep'", value);
}

function sandboxImage(image: unknown): string | { readonly snapshot: string } | undefined {
  if (image === undefined) return undefined;
  if (typeof image === 'string' && image.length > 0) return image;
  if (typeof image === 'object' && image !== null && Object.keys(image).length === 1) {
    const snapshot = (image as { snapshot?: unknown }).snapshot;
    if (typeof snapshot === 'string' && snapshot.length > 0) return Object.freeze({ snapshot });
  }
  throw invalid('sandbox', 'image', 'must be a VCR image name or { snapshot: id }', image);
}

function sandboxNetwork(network: unknown): SandboxNetwork {
  if (network === 'allow-all' || network === 'deny-all') return network;
  if (typeof network === 'object' && network !== null && Object.keys(network).length === 1 && Array.isArray((network as { allow?: unknown }).allow)) {
    const allow = (network as { allow: unknown[] }).allow;
    for (const domain of allow) {
      if (typeof domain !== 'string' || !DOMAIN.test(domain)) throw invalid('sandbox', 'network.allow', 'must list domains such as example.com or *.example.com', domain);
    }
    return Object.freeze({ allow: Object.freeze([...(allow as string[])]) });
  }
  throw invalid('sandbox', 'network', "must be 'allow-all', 'deny-all' or { allow: [domains] }", network);
}

function checkSandboxEnvVars(env: unknown): Readonly<Record<string, string>> {
  const entries = Object.entries(record('sandbox', env as object, 'env'));
  for (const [name, value] of entries) {
    if (!ENV_NAME.test(name)) throw invalid('sandbox', 'env', 'names must be shell variable names', name);
    if (isSecretName(name)) {
      throw new TypeError(`durable-harness: sandbox({ env }) refuses ${name}: a sandbox holds no secrets. Keep credentials in your Function and use them from a HostTool.`);
    }
    if (RESERVED_NAME.test(name)) throw new TypeError(`durable-harness: sandbox({ env }) refuses ${name}: the harness sets it`);
    if (typeof value !== 'string') throw invalid('sandbox', `env.${name}`, 'must be a string', value);
    const shape = secretValueShape(value);
    // The value is never part of the message: it may be the secret.
    if (shape) throw new TypeError(`durable-harness: sandbox({ env }) refuses ${name}: its value looks like ${shape}, and a sandbox holds no secrets. Keep credentials in your Function and use them from a HostTool.`);
  }
  return Object.freeze(Object.fromEntries(entries)) as Readonly<Record<string, string>>;
}

function sandboxBudget(budget: unknown): Readonly<Required<SandboxBudget>> {
  const b = record('sandbox', budget as object, 'budget');
  const keys = Object.keys(SANDBOX_DEFAULTS.budget) as (keyof SandboxBudget)[];
  checkKeys('sandbox', b, keys, {}, 'budget.');
  const out: Required<SandboxBudget> = { ...SANDBOX_DEFAULTS.budget };
  for (const key of keys) {
    const value = b[key];
    if (value === undefined) continue;
    if (typeof value !== 'number' || Number.isNaN(value) || value <= 0) throw invalid('sandbox', `budget.${key}`, 'must be a positive number (Infinity turns the cap off)', value);
    out[key] = value;
  }
  return Object.freeze(out);
}

function sandboxCredentials(credentials: unknown): Readonly<SandboxCredentials> {
  const c = record('sandbox', credentials as object, 'credentials');
  checkKeys('sandbox', c, ['token', 'teamId', 'projectId'], {}, 'credentials.');
  for (const key of ['token', 'teamId', 'projectId'] as const) {
    // The value is never part of the message: it may be the token.
    if (typeof c[key] !== 'string' || (c[key] as string).length === 0) throw new TypeError(`durable-harness: sandbox({ credentials }) needs a non-empty ${key}`);
  }
  return Object.freeze({ token: c.token as string, teamId: c.teamId as string, projectId: c.projectId as string });
}

// ---------------------------------------------------------------------------
// Shared checks

function brand<T extends object>(descriptor: T): T & { readonly [BRAND]: true } {
  Object.defineProperty(descriptor, BRAND, { value: true, enumerable: false });
  return Object.freeze(descriptor) as T & { readonly [BRAND]: true };
}

/**
 * The options to validate: a plain options object, or a descriptor of the
 * same kind (its own options, `credentials` included) with overrides on top.
 * A copy of a descriptor (`{ ...sandbox() }`) is refused by name, because it
 * has silently lost `credentials`.
 */
function merged(fn: 'virtual' | 'sandbox', first: unknown, overrides: unknown): Record<string, unknown> {
  const over = record(fn, overrides);
  if (isWorkspace(first)) {
    if (first.kind !== fn) throw new TypeError(`durable-harness: ${fn}(base, overrides) takes a ${fn}() descriptor as its base (got ${first.kind}())`);
    const base: Record<string, unknown> = { ...first };
    delete base.kind;
    const credentials = (first as { credentials?: unknown }).credentials;
    if (credentials !== undefined) base.credentials = credentials;
    return { ...base, ...over };
  }
  const opts = record(fn, first);
  if ('kind' in opts) {
    throw new TypeError(`durable-harness: ${fn}() got a copy of a descriptor: pass ${fn}() itself, or derive one with ${fn}(base, overrides) (a spread drops credentials and the brand)`);
  }
  if (Object.keys(over).length > 0) throw new TypeError(`durable-harness: ${fn}(options, overrides) takes a ${fn}() descriptor as its first argument; pass one options object instead`);
  return opts;
}

function record(fn: string, value: unknown, option?: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new TypeError(`durable-harness: ${fn}(${option ? `{ ${option} }` : ''}) takes an object (got ${describe(value)})`);
  }
  return value as Record<string, unknown>;
}

function checkKeys(fn: string, options: Record<string, unknown>, known: readonly string[], later: Record<string, string>, prefix = ''): void {
  for (const key of Object.keys(options)) {
    if (known.includes(key)) continue;
    const why = later[key];
    throw new TypeError(`durable-harness: ${fn}() has no option ${prefix}${key}${why ? `: ${why}` : ` (it takes ${known.map((k) => prefix + k).join(', ')})`}`);
  }
}

function int(fn: string, option: string, value: unknown, min: number, max: number): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < min || value > max) {
    throw invalid(fn, option, `must be a whole number from ${min} to ${max}`, value);
  }
  return value;
}

function invalid(fn: string, option: string, rule: string, value: unknown): TypeError {
  return new TypeError(`durable-harness: ${fn}({ ${option} }) ${rule} (got ${describe(value)})`);
}

function describe(value: unknown): string {
  if (typeof value === 'string') return JSON.stringify(value.length > 40 ? `${value.slice(0, 40)}…` : value);
  if (value === null || typeof value !== 'object') return String(value);
  return Array.isArray(value) ? 'an array' : 'an object';
}

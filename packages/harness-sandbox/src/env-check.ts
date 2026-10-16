/**
 * The check every spawn's `env` passes before it leaves the Function
 * (`MachineSpawn.env`, docs/workspaces.md §3.1 and decision 6): the one
 * channel through which a credential could reach the VM. The relay can't
 * judge a secret, so the machine builder (`machine.ts`) runs this on every
 * `spawn` and every harness inherits it.
 *
 * It applies `sandbox({ env })`'s name and value rules (the copy in
 * `durable-harness/src/workspace.ts`; harness-sandbox can't import
 * durable-harness, which bundles it, so the two are kept in step by hand), and
 * adds the attach-time rule: no value equal to a `process.env` value of 16 or
 * more characters. It does not apply §3.1's "names the harness sets" rule:
 * on a spawn the harness is the one setting `HOME` or `CODEX_HOME`. It does
 * refuse the names that change how a loader or shell starts (`LD_*`,
 * `BASH_ENV`, `ENV`, ...), which the relay also refuses on its side.
 *
 * A failing key is a `TypeError` naming the key and the rule, never the
 * value. Best effort, as §3.1 says: the model can read every env value.
 */

const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
/** Words that mark a credential anywhere in a name, and words that do as a whole `_` segment (as `workspace.ts`). */
const SECRET_WORD = /TOKEN|SECRET|PASSWORD|PASSWD|OIDC|CREDENTIAL|APIKEY|PRIVATE_?KEY/i;
const SECRET_SEGMENT = /^(?:KEY|KEYS|AUTH|PASS|COOKIE|BEARER|JWT|SIGNATURE|DSN|PAT|SK)$/i;
/** Names that change how a dynamic loader or a shell starts a process. The relay refuses them too (`relay.ts`). */
export const LOADER_NAMES = /^(?:LD_[A-Z0-9_]*|BASH_ENV|ENV|BASHOPTS|SHELLOPTS|PS4|PROMPT_COMMAND|BASH_FUNC_.*)$/;
/** Value shapes that are credentials whatever the name says (as `workspace.ts`'s `SECRET_VALUE_SHAPES`). */
const SECRET_VALUE_SHAPES: readonly (readonly [string, RegExp])[] = [
  ['a URL with a user or password', /^[a-z][a-z0-9+.-]*:\/\/[^/\s?#]*@/i],
  ['an OpenAI or Anthropic style key (sk-…)', /^sk-[A-Za-z0-9_-]{16,}/],
  ['a GitHub token', /^(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})/],
  ['a Slack token', /^xox[abposr]-/],
  ['an AWS access key id', /^(?:AKIA|ASIA)[A-Z0-9]{16}$/],
  ['a JWT', /^eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\./],
  ['a Vercel token', /^vc[pkri]_[A-Za-z0-9]{20,}/],
  ['a PEM private key', /-----BEGIN [A-Z ]*PRIVATE KEY-----/],
];

/**
 * Names that carry the per-attach model key `P` the CLI is configured with
 * (§5.5; `SessionMachine.modelKey`): exempt from the credential-name rule
 * only when their value **is** that key (never from the value rules), so the
 * name can't carry anything else into the VM. A `RemoteMachine.exec` env and
 * any spawn that isn't given the key refuse them as any credential-like
 * name. Claude Code's (A5): `ANTHROPIC_AUTH_TOKEN`; Codex's `env_key` (A6):
 * `HARNESS_CODEX_GATEWAY_KEY`.
 */
export const PLACEHOLDER_NAMES: ReadonlySet<string> = new Set<string>(['ANTHROPIC_AUTH_TOKEN', 'HARNESS_CODEX_GATEWAY_KEY']);

/**
 * Names whose values are paths, model ids or fixed names the harness sets
 * (never secrets): the `process.env` equality rule skips them, so a host whose
 * own `PATH` or `HOME` happens to be the VM's, whose `npm_package_name` is the
 * harness's client app name, or that runs under Claude Code with the session's
 * model in its own `ANTHROPIC_DEFAULT_*_MODEL`, isn't refused.
 */
const HARNESS_NAMES: ReadonlySet<string> = new Set([
  'PATH',
  'HOME',
  'TMPDIR',
  'XDG_CONFIG_HOME',
  'XDG_CACHE_HOME',
  'XDG_DATA_HOME',
  'XDG_STATE_HOME',
  'CLAUDE_CONFIG_DIR',
  'CODEX_HOME',
  'CLAUDE_AGENT_SDK_CLIENT_APP',
  'ANTHROPIC_DEFAULT_OPUS_MODEL',
  'ANTHROPIC_DEFAULT_SONNET_MODEL',
  'ANTHROPIC_DEFAULT_HAIKU_MODEL',
]);

const refuse = (key: string, why: string) => new TypeError(`harness-sandbox: spawn env ${JSON.stringify(key)} refused: ${why}`);

/**
 * Throw a `TypeError` for the first `env` entry the rules refuse. `hostEnv` is
 * the Function's own environment (default `process.env`): a value equal to
 * one of its values of 16+ characters is a credential leaving the Function.
 * `modelKey` is the attach's model key: a `PLACEHOLDER_NAMES` entry passes
 * the name rule only with exactly that value.
 */
export function checkSpawnEnv(env: Readonly<Record<string, string>> | undefined, hostEnv: Readonly<Record<string, string | undefined>> = process.env, modelKey?: string): void {
  if (env === undefined) return;
  if (typeof env !== 'object' || env === null || Array.isArray(env)) throw new TypeError('harness-sandbox: spawn env must be an object of strings');
  const hostValues = new Set<string>();
  for (const v of Object.values(hostEnv)) if (typeof v === 'string' && v.length >= 16) hostValues.add(v);
  for (const [key, value] of Object.entries(env)) {
    if (!ENV_NAME.test(key)) throw refuse(key, 'not a valid variable name');
    if (typeof value !== 'string') throw refuse(key, 'the value is not a string');
    if (value.includes('\0')) throw refuse(key, 'the value holds a NUL byte');
    if (LOADER_NAMES.test(key)) throw refuse(key, 'it changes how a loader or shell starts');
    const placeholder = PLACEHOLDER_NAMES.has(key) && modelKey !== undefined && modelKey.length >= 16 && value === modelKey;
    if (!placeholder && (SECRET_WORD.test(key) || key.split('_').some((s) => SECRET_SEGMENT.test(s)))) throw refuse(key, PLACEHOLDER_NAMES.has(key) ? "a credential-like name, whose value is not this attach's model key" : 'a credential-like name');
    for (const [what, shape] of SECRET_VALUE_SHAPES) if (shape.test(value)) throw refuse(key, `the value looks like ${what}`);
    if (hostValues.has(value) && !HARNESS_NAMES.has(key)) throw refuse(key, "the value equals one of the Function's own environment values");
  }
}

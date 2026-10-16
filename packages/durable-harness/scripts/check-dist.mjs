#!/usr/bin/env node
// After `pnpm build`, the release gate (`pnpm ci:publish` and .github/workflows/release.yml run it):
// - every bare import in dist is a Node built-in, a dependency or a peer, and every entry exists;
// - no entry still holds a stub (src/unreleased.ts: an entry that exists before the code behind it);
// - no published file carries a test module: the stand-in VM (`localMachine`, `brandForTests`), the fake
//   Sandbox API, a harness's stand-in (docs/workspaces.md §6 rule 2);
// - Claude Code and Codex can't be built without a branded machine: the machine brand is claimed once, by the
//   session factory, each harness's drive checks it, and the published builders refuse every other way in
//   (`virtual()`, a copied descriptor, the internal `sandbox`/`machine`/`test` options) (§6 rules 1 and 2);
// - the relay bundle ships, matches its hash, and its arm reply says `modelAuth: true` (the model port checks
//   the CLI's key and its caller's pid). That needs Linux's /proc: elsewhere it is checked only with
//   DH_RELEASE_GATE=1, which the release job sets, and otherwise reported as not checked.
import { spawn, spawnSync } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { builtinModules } from 'node:module';
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { gunzipSync } from 'node:zlib';

// The package to check: this script's, or the directory given (test/release-gate.test.ts's fixtures).
const root = process.argv[2] ? resolve(process.argv[2]) : dirname(dirname(fileURLToPath(import.meta.url)));
const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
const declared = new Set([pkg.name, ...Object.keys(pkg.dependencies ?? {}), ...Object.keys(pkg.peerDependencies ?? {})]);
const packageOf = (spec) => (spec.startsWith('@') ? spec.split('/').slice(0, 2).join('/') : spec.split('/')[0]);
const builtins = new Set(builtinModules);
let bad = 0;
for (const file of readdirSync(join(root, 'dist'), { recursive: true }).filter((f) => /\.(js|d\.ts)$/.test(f))) {
  // A JSDoc line (` * ...`) is a comment, whatever it quotes (an example's `await import('workflow/runtime')`).
  const text = readFileSync(join(root, 'dist', file), 'utf8')
    .split('\n')
    .filter((line) => !/^\s*\*(?:\s|\/|$)/.test(line))
    .join('\n');
  if (file.endsWith('.d.ts') && /(?:from\s*|import\(\s*)['"]@vercel\/agent-runs(?:\/[^'"]*)?['"]/.test(text)) {
    console.error(`${file} references optional agent-runs types instead of inlining them`);
    bad++;
  }
  for (const [, fromSpec, dynamicSpec] of text.matchAll(/(?:^|[\s;])(?:import|export)\b[^'"]*?from\s*['"]([^'"./][^'"]*)['"]|import\(\s*['"]([^'"./][^'"]*)['"]\s*\)/gm)) {
    const spec = fromSpec ?? dynamicSpec;
    if (!spec) continue;
    const name = spec.startsWith('node:') ? spec.slice(5) : spec;
    if (builtins.has(name) || builtins.has(name.split('/')[0])) continue;
    if (!declared.has(packageOf(spec))) {
      console.error(`${file} imports ${spec}, which package.json does not declare`);
      bad++;
    }
  }
}
// Must match src/unreleased.ts.
const UNRELEASED = 'durable-harness: not released yet';
for (const file of readdirSync(join(root, 'dist'), { recursive: true }).filter((f) => /\.js$/.test(f))) {
  if (readFileSync(join(root, 'dist', file), 'utf8').includes(UNRELEASED)) {
    console.error(`${file} still holds a stub that is not released yet (src/unreleased.ts): the code behind it has to land before a release`);
    bad++;
  }
}
// The workspace manifest publishes `publishConfig.exports`; a packed one has them as `exports`.
for (const [sub, target] of Object.entries(pkg.publishConfig?.exports ?? pkg.exports)) {
  for (const path of typeof target === 'string' ? [target] : Object.values(target)) {
    try {
      readFileSync(join(root, path));
    } catch {
      console.error(`${sub}: ${path} is missing`);
      bad++;
    }
  }
}
const dist = join(root, 'dist');
const js = readdirSync(dist, { recursive: true }).filter((f) => /\.js$/.test(f));
const read = (file) => readFileSync(join(dist, file), 'utf8');
const fail = (message) => {
  console.error(message);
  bad++;
};

// No test module in any published file. esbuild names each module's source in a comment (`// ../harness-sandbox/src/session.ts`),
// so a module from a `test/` directory shows by its path; the stand-in's and the fake API's own names are checked too.
const TEST_MODULE = /^\/\/ (?:\.\.\/)*[\w.-]+\/test\/\S+$/m;
const STAND_IN = ['brandForTests', 'standInGuard', 'localMachine', 'FakeSandboxApi', 'harness-world-sandbox/testing', 'harness-world-claude-code/testing'];
for (const file of js) {
  const text = read(file);
  const module = TEST_MODULE.exec(text);
  if (module) fail(`${file} carries a test module (${module[0].slice(3)}): no published entry may (docs/workspaces.md §6 rule 2)`);
  for (const name of STAND_IN) if (text.includes(name)) fail(`${file} carries ${name}, the stand-in's: no published entry may (§6 rule 2)`);
}

// The machine brand: one minter, claimed once at load by the session factory, and every Claude Code or Codex drive checks it.
const minters = js.filter((f) => read(f).includes("the session machine brand is already claimed"));
const claims = js.flatMap((f) => [...read(f).matchAll(/^var \w+ = (machineBrand\d*)\(\);$/gm)].map((m) => `${f}: ${m[0]}`));
if (minters.length !== 1) fail(`the machine brand's minter is in ${minters.length} files (${minters.join(', ')}), not one`);
if (claims.length !== 1) fail(`the machine brand is claimed ${claims.length} times at load (${claims.join('; ')}), not once, by the session factory`);
else if (!read(claims[0].split(':')[0]).includes('// ../harness-sandbox/src/session.ts')) fail(`the machine brand is claimed outside the session factory: ${claims[0]}`);
// Each refusal is the throw of the drive's own brand check: `if (!isBrandedMachine(machine)) throw sandboxRequired(...)` (Claude
// Code's sandboxRequired() holds its message; Codex's drive closes what it opened first), never a message somewhere in the file.
const escape = (t) => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
for (const [harness, refusal, check] of [
  ['Claude Code', 'Claude Code runs only in a Vercel Sandbox', /if \(!isBrandedMachine\d*\(\w+\)\) throw (sandboxRequired\d*)\(\);/g],
  ['Codex', "the machine it was given is not one the sandbox tier's session factory made", /if \(!isBrandedMachine\d*\(\w+\)\) \{[\s\S]{0,600}?throw (sandboxRequired\d*)\(("[^"]*")\);\s*\}/g],
]) {
  const files = js.filter((f) => read(f).includes(refusal));
  if (files.length === 0) fail(`${harness}'s refusal of an unbranded machine (sandbox_required) is in no published file`);
  for (const f of files) {
    const text = read(f);
    const guarded = [...text.matchAll(check)].some((m) => {
      // The message is the throw's argument, or (Claude Code) in the body of the function it calls.
      if (m[2] !== undefined) return m[2].includes(refusal);
      const body = new RegExp(`function ${escape(m[1])}\\(\\) \\{[^}]{0,600}`).exec(text)?.[0] ?? '';
      return body.includes(refusal);
    });
    if (!guarded) fail(`${f} has ${harness}'s refusal, but not as the throw of a drive's brand check (if (!isBrandedMachine(machine)) throw sandboxRequired(...))`);
  }
}
// The published builders: no way in but workspace: sandbox().
try {
  const { claudeCode, claudeCodeHarness } = await import(pathToFileURL(join(dist, 'claude-code.js')).href);
  const { codex, codexHarness } = await import(pathToFileURL(join(dist, 'codex.js')).href);
  const { virtual, sandbox } = await import(pathToFileURL(join(dist, 'index.js')).href);
  const handWritten = { brand: {}, spawn: () => {}, files: {}, dirs: {}, workspace: {}, modelKey: 'dhm-x', notes: [] };
  const refusals = [
    ['claudeCodeHarness({ workspace: virtual() })', () => claudeCodeHarness({ workspace: virtual() })],
    ['claudeCodeHarness({ workspace: { ...sandbox() } })', () => claudeCodeHarness({ workspace: { ...sandbox() } })],
    ['claudeCodeHarness({ sandbox })', () => claudeCodeHarness({ sandbox: { attach: async () => handWritten, acknowledge: async () => {} } })],
    ['claudeCodeHarness({ host })', () => claudeCodeHarness({ host: {} })],
    // Claude's sandbox off: a command could read the model key from the CLI's environ, or take its socket (decision 6).
    ['claudeCodeHarness({ unsandboxedCommands: true })', () => claudeCodeHarness({ unsandboxedCommands: true })],
    // A caller's Agent SDK: its query() could start the CLI on the host.
    ['claudeCodeHarness({ sdk })', () => claudeCodeHarness({ sdk: { query: () => {} } })],
    ['claudeCode({ executable })', () => claudeCode({ model: 'm', executable: '/bin/sh' })],
    ['claudeCode({ cwd })', () => claudeCode({ model: 'm', cwd: '/tmp' })],
    ['claudeCode({ env })', () => claudeCode({ model: 'm', env: { A: 'b' } })],
    ['claudeCode({ workspace: virtual() })', () => claudeCode({ model: 'm', workspace: virtual() })],
    ['codexHarness({ host })', () => codexHarness({ host: {} })],
    ['codexHarness({ workspace: virtual() })', () => codexHarness({ workspace: virtual() })],
    ['codexHarness({ workspace: { ...sandbox() } })', () => codexHarness({ workspace: { ...sandbox() } })],
    ['codexHarness({ machine })', () => codexHarness({ machine: { acquire: async () => handWritten } })],
    ['codexHarness({ test })', () => codexHarness({ test: { command: '/bin/sh' } })],
    ['codex({ workspace: virtual() })', () => codex({ model: 'm', workspace: virtual() })],
  ];
  for (const [what, make] of refusals) {
    let threw;
    try {
      make();
    } catch (err) {
      threw = err;
    }
    if (!(threw instanceof TypeError)) fail(`${what} builds a harness from dist (${threw ? `it threw ${threw}` : 'no error'}): only workspace: sandbox() may reach the session factory`);
  }
} catch (err) {
  fail(`the published Claude Code and Codex builders could not be checked: ${err?.stack ?? err}`);
}

// The relay bundle ships, in one file, and matches its hash.
const MARKER = 'dh-shipped-relay-bundle/v1';
const relayFiles = js.filter((f) => read(f).includes(MARKER));
let relay;
if (relayFiles.length !== 1) fail(`the relay bundle is in ${relayFiles.length} files of dist, not one (src/relay-bundle.ts, filled in by tsup.config.ts)`);
else {
  const { SHIPPED_RELAY } = await import(pathToFileURL(join(dist, relayFiles[0])).href);
  if (!SHIPPED_RELAY || SHIPPED_RELAY.marker !== MARKER) fail(`${relayFiles[0]} does not export the shipped relay bundle`);
  else {
    const bytes = gunzipSync(Buffer.from(SHIPPED_RELAY.gzip, 'base64'));
    if (createHash('sha256').update(bytes).digest('hex') !== SHIPPED_RELAY.version) fail('the shipped relay bundle does not match its hash');
    else if (bytes.length < 50_000 || !bytes.toString('utf8', 0, 4096).includes('createRequire')) fail(`the shipped relay bundle (${bytes.length} bytes) is not the relay`);
    else relay = { bytes, version: SHIPPED_RELAY.version };
  }
}

// Its model port authenticates: an arm with a model key's hash says `modelAuth: true`, and the port answers as it says: no key
// 401, a wrong key 401, the right key from a process the attach didn't spawn (this one) 403.
const required = process.platform === 'linux' || process.env.DH_RELEASE_GATE === '1';
if (relay && required) {
  try {
    const { reply, answers } = await armShippedRelay(relay.bytes);
    if (reply?.modelAuth !== true) fail(`the shipped relay's arm reply says modelAuth: ${reply?.modelAuth}: the model port doesn't check the CLI's key and its caller (a release needs both)`);
    const want = { 'no key': 401, 'a wrong key': 401, "the key, from a process the attach didn't spawn": 403 };
    for (const [what, status] of Object.entries(want)) if (answers[what] !== status) fail(`the shipped relay's model port answered ${answers[what]} to a call with ${what}, not ${status}`);
  } catch (err) {
    fail(`the shipped relay could not be armed: ${err?.message ?? err}`);
  }
} else if (relay) {
  console.warn(`dist: modelAuth not checked on ${process.platform} (the relay's caller check needs Linux's /proc; the release job checks it)`);
}

if (bad) process.exit(1);
console.log(`dist: every import is declared, every entry exists, no stub or stand-in is published, Claude Code and Codex need a branded machine, and the relay bundle ships${relay && required ? ' with modelAuth' : ''}`);

/** Run the shipped relay over temp dirs as this user, arm it with a model key's hash, and return its reply and what its model port answered. */
async function armShippedRelay(bytes) {
  const dir = mkdtempSync(join(tmpdir(), 'dh-gate-relay-'));
  const file = join(dir, 'relay.mjs');
  writeFileSync(file, bytes, { mode: 0o600 });
  const port = async () =>
    new Promise((resolve, reject) => {
      const server = createServer();
      server.listen(0, '127.0.0.1', () => {
        const { port } = server.address();
        server.close(() => resolve(port));
      });
      server.on('error', reject);
    });
  const config = { port: await port(), modelPort: await port(), stateDir: join(dir, 'state'), workspace: join(dir, 'workspace'), cacheRoot: join(dir, 'cache'), relayVersion: 'gate', cliVersions: {} };
  const configPath = join(dir, 'relay.json');
  writeFileSync(configPath, JSON.stringify(config));
  const serve = spawn(process.execPath, [file, 'serve', configPath], { stdio: ['ignore', 'ignore', 'pipe'] });
  let stderr = '';
  serve.stderr.on('data', (d) => (stderr += d));
  try {
    for (let i = 0; !existsSync(join(config.stateDir, 'control.sock')); i++) {
      if (serve.exitCode !== null || i > 400) throw new Error(`the relay did not start: ${stderr.slice(0, 500)}`);
      await new Promise((r) => setTimeout(r, 25));
    }
    const sha = (b) => createHash('sha256').update(b).digest('hex');
    const token = randomBytes(32).toString('base64url');
    const key = `dhm-${randomBytes(24).toString('base64url')}`;
    const armed = spawnSync(process.execPath, [file, 'arm', configPath, '--sha256', sha(Buffer.from(token, 'base64url')), '--session', 'gate', '--fence', '1', '--fenced', 'true', '--model', sha(Buffer.from(key, 'utf8'))], { encoding: 'utf8', timeout: 30_000 });
    const last = armed.stdout.trim().split('\n').at(-1);
    if (!last) throw new Error(`no arm reply: ${armed.stderr.slice(0, 500)}`);
    const reply = JSON.parse(last);
    // The port, end to end: the caller is checked before anything else (no /model link is dialed here).
    const call = async (auth) => {
      const r = await fetch(`http://127.0.0.1:${config.modelPort}/v1/messages`, { method: 'POST', headers: { 'content-type': 'application/json', ...(auth ? { authorization: `Bearer ${auth}` } : {}) }, body: '{}' });
      await r.arrayBuffer();
      return r.status;
    };
    const answers = { 'no key': await call(''), 'a wrong key': await call(`dhm-${randomBytes(24).toString('base64url')}`), "the key, from a process the attach didn't spawn": await call(key) };
    return { reply, answers };
  } finally {
    serve.kill('SIGKILL');
    rmSync(dir, { recursive: true, force: true });
  }
}

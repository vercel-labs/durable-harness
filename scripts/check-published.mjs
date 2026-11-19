#!/usr/bin/env node
/**
 * Fails when a publishable package version on this commit is not on npm.
 *
 * Adapted from vercel/workflow's scripts/check-published.mjs. The Release
 * workflow (.github/workflows/release.yml) runs it after its publish step,
 * whatever that step's outcome: every non-private package that changesets does
 * not ignore (here: durable-harness) must have its manifest version in the
 * registry's version list, and the branch's dist-tag (the pre-release tag from
 * `.changeset/pre.json` while in pre mode, `latest` otherwise) must point at
 * it. The invariant holds on every commit of main once the first version is
 * out, not only right after a publish, so a gap keeps failing the job until it
 * is closed. (Before the first, hand-made publish the Release workflow skips
 * this check: docs/releasing.md.)
 *
 * Manifests are read from the commit (`git show HEAD:...`), never from the
 * working tree: the Release job's version step rewrites every package.json in
 * the runner's working tree to the next version when it opens the "Version
 * Packages" PR, and a working-tree read after it would ask npm for a version
 * that is not supposed to exist yet.
 *
 * npm does not commit a publish synchronously: `pnpm publish` exits 0 once the
 * version is staged, and the version list and dist-tag catch up afterwards,
 * minutes later for large tarballs (vercel/workflow measured 1.5 to 5 minutes).
 * So the check retries for about ten minutes before it calls a gap real.
 *
 * Usage: node scripts/check-published.mjs [--tag <dist-tag>] [--ref <git-ref>]
 */

import { execFileSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const REGISTRY = 'https://registry.npmjs.org';
// 20 attempts, 30s apart: ~9.5 minutes of waiting. Costs nothing when the
// registry is already caught up, since the loop exits as soon as nothing is pending.
const ATTEMPTS = Number(process.env.CHECK_PUBLISHED_ATTEMPTS ?? 20);
const DELAY_MS = Number(process.env.CHECK_PUBLISHED_DELAY_MS ?? 30_000);

function argValue(flag) {
  const index = process.argv.indexOf(flag);
  return index !== -1 ? process.argv[index + 1] : undefined;
}

// The commit under test. Manifests come from here, not from disk (see above).
const REF = argValue('--ref') ?? 'HEAD';

function git(args) {
  // stderr piped: a missing .changeset/pre.json is the usual case, not an error to print.
  return execFileSync('git', args, { cwd: root, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'] });
}

function readJsonAtRef(path) {
  try {
    return JSON.parse(git(['show', `${REF}:${path}`]));
  } catch (error) {
    if (error.status === 128) return undefined; // not in that commit
    throw error;
  }
}

function distTagForBranch() {
  const explicit = argValue('--tag');
  if (explicit) return explicit;
  // `pre.json` existing at all (in `pre` or `exit` mode) means the versions on
  // this commit are still pre-release versions published under its tag, until
  // the "Version Packages" PR writes the final versions and deletes it.
  const pre = readJsonAtRef('.changeset/pre.json');
  if (pre?.tag) return pre.tag;
  return 'latest';
}

function escapeRegExp(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function publishablePackages() {
  const config = readJsonAtRef('.changeset/config.json');
  if (!config) {
    throw new Error(`Cannot read .changeset/config.json at ${REF}. This script reads the commit, not the working tree, so it needs a checkout with git history.`);
  }
  const { ignore = [] } = config;
  const ignored = new Set(ignore.filter((name) => !name.includes('*')));
  const ignoredPatterns = ignore.filter((name) => name.includes('*')).map((glob) => new RegExp(`^${glob.split('*').map(escapeRegExp).join('.*')}$`));
  const manifestPaths = git(['ls-tree', '-r', '--name-only', REF, '--', 'packages/', 'apps/'])
    .split('\n')
    .filter((path) => /^(packages|apps)\/[^/]+\/package\.json$/.test(path));
  const result = [];
  for (const manifestPath of manifestPaths) {
    const manifest = readJsonAtRef(manifestPath);
    if (!manifest?.name || !manifest.version || manifest.private) continue;
    if (ignored.has(manifest.name)) continue;
    if (ignoredPatterns.some((re) => re.test(manifest.name))) continue;
    result.push({ name: manifest.name, version: manifest.version });
  }
  return result.sort((a, b) => a.name.localeCompare(b.name));
}

async function registryState(name) {
  const url = `${REGISTRY}/${name.replace('/', '%2F')}`;
  const response = await fetch(url, { headers: { accept: 'application/json', 'cache-control': 'no-cache' } });
  if (response.status === 404) return { versions: {}, distTags: {} };
  if (!response.ok) throw new Error(`GET ${url} failed: ${response.status} ${response.statusText}`);
  const doc = await response.json();
  return { versions: doc.versions ?? {}, distTags: doc['dist-tags'] ?? {} };
}

async function check(pkg, tag) {
  const { versions, distTags } = await registryState(pkg.name);
  const visible = Object.hasOwn(versions, pkg.version);
  const tagged = distTags[tag] === pkg.version;
  return { ...pkg, visible, tagged, actualTag: distTags[tag] };
}

const tag = distTagForBranch();
const packages = publishablePackages();
if (packages.length === 0) {
  console.log('No publishable packages on this commit.');
  process.exit(0);
}
let results = await Promise.all(packages.map((pkg) => check(pkg, tag)));

for (let attempt = 1; attempt < ATTEMPTS; attempt++) {
  const pending = results.filter((r) => !(r.visible && r.tagged));
  if (pending.length === 0) break;
  console.log(`${pending.length} package(s) not yet visible with the "${tag}" tag; re-checking in ${DELAY_MS / 1000}s (attempt ${attempt + 1}/${ATTEMPTS})...`);
  await new Promise((resolveDelay) => setTimeout(resolveDelay, DELAY_MS));
  const rechecked = await Promise.all(pending.map((r) => check(r, tag)));
  results = results.map((r) => rechecked.find((n) => n.name === r.name) ?? r);
}

const width = Math.max(...results.map((r) => r.name.length));
for (const r of results) {
  const status = r.visible ? (r.tagged ? 'ok' : `visible, but "${tag}" is ${r.actualTag ?? 'unset'}`) : 'NOT ON NPM';
  console.log(`${r.name.padEnd(width)}  ${r.version.padEnd(14)}  ${status}`);
}

const missing = results.filter((r) => !r.visible);
const untagged = results.filter((r) => r.visible && !r.tagged);
if (missing.length === 0 && untagged.length === 0) {
  console.log(`\n✓ all ${results.length} publishable package(s) are on npm under the "${tag}" tag.`);
  process.exit(0);
}

console.error('');
if (missing.length > 0) {
  console.error(`✗ ${missing.length} package version(s) on this commit are not on npm:`);
  for (const r of missing) console.error(`    ${r.name}@${r.version}`);
  console.error(
    '\n  Read the publish step above. If `pnpm publish` reported the version as\n' +
      '  "previously staged", it is waiting for a maintainer to approve it in the\n' +
      "  package's Staged Packages tab on npmjs.com. If it failed with\n" +
      '  `E404 Not Found - PUT https://registry.npmjs.org/<package>`, npm did not\n' +
      "  accept the job's OIDC token: the package's Trusted Publisher on npmjs.com\n" +
      '  is missing or does not match this repository and `release.yml`\n' +
      '  (docs/releasing.md, "When a publish fails"). Otherwise re-run the Release\n' +
      '  workflow (workflow_dispatch); `changeset publish` skips versions already live.'
  );
}
if (untagged.length > 0) {
  console.error(`✗ ${untagged.length} package(s) are on npm but the "${tag}" dist-tag does not point at them:`);
  for (const r of untagged) console.error(`    ${r.name}@${r.version} (tag is ${r.actualTag ?? 'unset'})`);
}
process.exit(1);

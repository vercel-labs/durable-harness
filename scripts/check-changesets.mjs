#!/usr/bin/env node
/**
 * Fails when the pending changesets would take a published package off 0.0.x,
 * or cannot be turned into a release plan at all.
 *
 * The rule for durable-harness (docs/releasing.md): while it is on
 * 0.0.x, every release is a patch. A `minor` changeset would make the next
 * "Version Packages" PR 0.1.0, a `major` one 1.0.0; this check rejects both on
 * the pull request that adds them, before they reach main. It also fails when a
 * published package's version on this commit is outside 0.0.x, which only a
 * hand edit of its package.json can do.
 *
 * Adapted from vercel/workflow's .github/scripts/check-release-major.js (every
 * public package stays on the release line) and scripts/check-changesets.mjs
 * (assemble the same release plan `changeset version` assembles, with the same
 * libraries, declared at the ranges @changesets/cli declares, and stop before
 * the network-bound changelog step). The plan also catches a changeset that
 * names a package not in the workspace, which would break `changeset version`
 * on main. A changeset naming a private package (every package but
 * durable-harness) fails too: it would release nothing.
 *
 * Leaving 0.0.x requires an explicit human maintainer decision and a separately
 * reviewed policy change. A normal release request never authorizes changing
 * LINE below or relaxing this guard (AGENTS.md, docs/releasing.md).
 *
 * Usage: node scripts/check-changesets.mjs (pnpm check:changesets). Writes a
 * summary to $GITHUB_STEP_SUMMARY when set.
 */

import { appendFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { assembleReleasePlan } from '@changesets/assemble-release-plan';
import { readConfig } from '@changesets/config';
import { readPreState } from '@changesets/pre';
import { readChangesets } from '@changesets/read';
import { getPackages } from '@manypkg/get-packages';

/** The release line every published package stays on: 0.0.x, patch releases only. */
const LINE = /^0\.0\.\d+(?:-[0-9A-Za-z.-]+)?$/;
const LINE_NAME = '0.0.x';

const cwd = resolve(dirname(fileURLToPath(import.meta.url)), '..');

function summarize(markdown) {
  process.stdout.write(markdown);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, markdown);
}

const packages = await getPackages(cwd);
const { config, errors: configErrors, warnings: configWarnings } = await readConfig(cwd, packages);
for (const warning of configWarnings ?? []) console.warn(`! .changeset/config.json: ${warning}`);
if (configErrors) {
  console.error('✗ .changeset/config.json is invalid:');
  for (const error of configErrors) console.error(`    ${error}`);
  process.exit(1);
}
const [changesets, preState] = await Promise.all([readChangesets(cwd), readPreState(cwd)]);

let plan;
try {
  plan = assembleReleasePlan(changesets, packages, config, preState);
} catch (error) {
  console.error('✗ The pending changesets cannot be turned into a release plan.');
  console.error('  `changeset version` will fail on main with this error:\n');
  console.error(
    String(error?.message ?? error)
      .split('\n')
      .map((line) => `    ${line}`)
      .join('\n')
  );
  console.error('\n  A changeset may only list durable-harness (every other workspace package is private).');
  process.exit(1);
}

const ignored = new Set(config.ignore ?? []);
const published = packages.packages
  .filter((pkg) => pkg.packageJson.private !== true && !ignored.has(pkg.packageJson.name))
  .map((pkg) => ({ name: pkg.packageJson.name, version: pkg.packageJson.version, path: pkg.relativeDir }));

const problems = [];
// A changeset naming a private package releases nothing (they are not versioned,
// .changeset/config.json#privatePackages), and is silently consumed by the next
// `changeset version`. The kernel and the adapters ship inside durable-harness's
// bundle, so a change to them is a durable-harness changeset.
const publishedNames = new Set(published.map((pkg) => pkg.name));
for (const changeset of changesets) {
  for (const release of changeset.releases) {
    if (publishedNames.has(release.name)) continue;
    problems.push({ name: release.name, what: 'private: this releases nothing; name `durable-harness`, which bundles it', where: `.changeset/${changeset.id}.md` });
  }
}
for (const pkg of published) {
  if (!LINE.test(pkg.version)) {
    problems.push({ name: pkg.name, what: `version on this commit is \`${pkg.version}\`, not ${LINE_NAME}`, where: `${pkg.path}/package.json` });
    continue;
  }
  for (const changeset of changesets) {
    for (const release of changeset.releases) {
      if (release.name !== pkg.name || release.type === 'patch' || release.type === 'none') continue;
      problems.push({ name: pkg.name, what: `\`${release.type}\` bump (only \`patch\` while on ${LINE_NAME})`, where: `.changeset/${changeset.id}.md` });
    }
  }
  const release = plan.releases.find((r) => r.name === pkg.name);
  if (release && release.type !== 'none' && !LINE.test(release.newVersion)) {
    problems.push({ name: pkg.name, what: `the next version would be \`${release.newVersion}\`, off ${LINE_NAME}`, where: 'release plan' });
  }
}

const releasing = plan.releases.filter((r) => r.type !== 'none');
const next = releasing.map((r) => `\`${r.name}@${r.newVersion}\``).join(', ') || 'nothing';
if (problems.length === 0) {
  summarize(
    `### Release line\n\nEvery published package is on ${LINE_NAME} and every pending changeset is a patch. ${changesets.length} changeset(s) pending${preState ? ` (${preState.mode} mode, tag "${preState.tag}")` : ''}; next release: ${next}.\n`
  );
  process.exit(0);
}

summarize(
  [
    '### Release line',
    '',
    `Failing: changesets name only published packages, and those stay on ${LINE_NAME}, patch releases only (docs/releasing.md).`,
    '',
    '| Package | Problem | Where |',
    '| --- | --- | --- |',
    ...problems.map((p) => `| \`${p.name}\` | ${p.what} | \`${p.where}\` |`),
    '',
  ].join('\n')
);
process.exit(1);

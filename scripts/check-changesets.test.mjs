import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const script = readFileSync(join(root, 'scripts/check-changesets.mjs'));
const config = JSON.parse(readFileSync(join(root, '.changeset/config.json'), 'utf8'));

// Run the real lint against disposable workspaces, including Changesets' real
// release planner. No fixture can change the checked-out package's version.
function check(t, { version = '0.0.1', bump, name = 'durable-harness', preState } = {}) {
  const cwd = mkdtempSync(join(tmpdir(), 'durable-harness-release-policy-'));
  t.after(() => rmSync(cwd, { recursive: true, force: true }));
  for (const dir of ['scripts', '.changeset', 'packages/durable-harness', 'packages/private']) {
    mkdirSync(join(cwd, dir), { recursive: true });
  }
  writeFileSync(join(cwd, 'package.json'), JSON.stringify({ name: 'policy-fixture', private: true, packageManager: 'pnpm@11.24.0' }));
  writeFileSync(join(cwd, 'pnpm-workspace.yaml'), 'packages:\n  - packages/*\n');
  writeFileSync(join(cwd, 'packages/durable-harness/package.json'), JSON.stringify({ name: 'durable-harness', version }));
  writeFileSync(join(cwd, 'packages/private/package.json'), JSON.stringify({ name: 'private-harness', version: '0.0.1', private: true }));
  writeFileSync(join(cwd, '.changeset/config.json'), JSON.stringify({ ...config, changelog: false }));
  if (bump) writeFileSync(join(cwd, '.changeset/fixture.md'), `---\n"${name}": ${bump}\n---\n\nFixture release.\n`);
  if (preState) writeFileSync(join(cwd, '.changeset/pre.json'), JSON.stringify(preState));
  writeFileSync(join(cwd, 'scripts/check-changesets.mjs'), script);
  symlinkSync(join(root, 'node_modules'), join(cwd, 'node_modules'), 'junction');
  const env = { ...process.env };
  delete env.GITHUB_STEP_SUMMARY;
  const result = spawnSync(process.execPath, [join(cwd, 'scripts/check-changesets.mjs')], { cwd, env, encoding: 'utf8', timeout: 30_000 });
  assert.ifError(result.error);
  return { status: result.status, output: result.stdout + result.stderr };
}

test('a patch changeset releases 0.0.2', (t) => {
  const result = check(t, { bump: 'patch' });
  assert.equal(result.status, 0, result.output);
  assert.match(result.output, /durable-harness@0\.0\.2/);
});

for (const bump of ['minor', 'major']) {
  test(`rejects a ${bump} changeset before versioning`, (t) => {
    const result = check(t, { bump });
    assert.equal(result.status, 1, result.output);
    assert.match(result.output, new RegExp('`' + bump + '` bump'));
  });
}

for (const version of ['0.1.0', '0.1.0-beta.1', '1.0.0', '1.0.0-beta.1']) {
  test(`rejects a manifest edited directly to ${version}, even with no changesets`, (t) => {
    const result = check(t, { version });
    assert.equal(result.status, 1, result.output);
    assert.match(result.output, /version on this commit/);
    assert.match(result.output, /not 0\.0\.x/);
  });
}

test('a consumed version PR can publish the next patch without pending changesets', (t) => {
  const result = check(t, { version: '0.0.2' });
  assert.equal(result.status, 0, result.output);
  assert.match(result.output, /0 changeset\(s\) pending/);
});

test('allows a patch within an explicitly configured beta prerelease', (t) => {
  const result = check(t, {
    version: '0.0.2-beta.0', bump: 'patch',
    preState: { mode: 'pre', tag: 'beta', initialVersions: { 'durable-harness': '0.0.1' }, changesets: [] },
  });
  assert.equal(result.status, 0, result.output);
  assert.match(result.output, /durable-harness@0\.0\.2-beta\.1/);
});

test('rejects changesets for private bundled packages', (t) => {
  const result = check(t, { bump: 'patch', name: 'private-harness' });
  assert.equal(result.status, 1, result.output);
  assert.match(result.output, /private: this releases nothing/);
});

test('rejects a changeset that cannot form a release plan', (t) => {
  const result = check(t, { bump: 'patch', name: 'missing-harness' });
  assert.equal(result.status, 1, result.output);
  assert.match(result.output, /cannot be turned into a release plan/);
});

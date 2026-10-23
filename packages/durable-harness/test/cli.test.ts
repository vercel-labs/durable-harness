import { spawnSync } from 'node:child_process';
import { mkdtempSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, describe, expect, it } from 'vitest';

const CLI = fileURLToPath(new URL('../src/cli.ts', import.meta.url));
const cwd = mkdtempSync(join(tmpdir(), 'durable-harness-cli-help-'));
afterAll(() => rmSync(cwd, { recursive: true, force: true }));

const run = (args: string[]) => spawnSync(process.execPath, [CLI, ...args], { cwd, encoding: 'utf8' });

describe('explicit CLI help', () => {
  for (const command of [[], ['vercel-json'], ['sandbox'], ['sandbox', 'prepare']]) {
    for (const flag of ['--help', '-h']) {
      const args = [...command, flag];
      it(`${args.join(' ')} prints help to stdout and succeeds without doing work`, () => {
        const result = run(args);
        expect(result.error).toBeUndefined();
        expect(result.status).toBe(0);
        expect(result.stdout).toMatch(/^usage: durable-harness /);
        expect(result.stdout).toContain('durable-harness sandbox prepare');
        expect(result.stderr).toBe('');
        expect(readdirSync(cwd)).toEqual([]);
      });
    }
  }

  for (const args of [['bogus'], ['sandbox'], ['sandbox', 'bogus']]) {
    it(`${args.join(' ')} remains an error with usage on stderr`, () => {
      const result = run(args);
      expect(result.error).toBeUndefined();
      expect(result.status).toBe(1);
      expect(result.stdout).toBe('');
      expect(result.stderr).toMatch(/^usage: durable-harness /);
    });
  }

  it('keeps no-argument usage on stderr with a successful exit', () => {
    const result = run([]);
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(0);
    expect(result.stdout).toBe('');
    expect(result.stderr).toMatch(/^usage: durable-harness /);
  });
});

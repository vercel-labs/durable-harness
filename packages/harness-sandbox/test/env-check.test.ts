import { describe, expect, it } from 'vitest';
import { checkProvisionEnv, checkSpawnEnv } from '../src/env-check.ts';

describe('checkSpawnEnv', () => {
  it("passes Claude Code's model ids when the host holds the same ones (next dev run from Claude Code), and still refuses a host value under another name", () => {
    const host = { ANTHROPIC_DEFAULT_SONNET_MODEL: 'claude-sonnet-5-5', ANTHROPIC_MODEL: 'claude-sonnet-5-5' };
    const ids = { ANTHROPIC_DEFAULT_OPUS_MODEL: 'claude-sonnet-5-5', ANTHROPIC_DEFAULT_SONNET_MODEL: 'claude-sonnet-5-5', ANTHROPIC_DEFAULT_HAIKU_MODEL: 'claude-sonnet-5-5' };
    expect(() => checkSpawnEnv(ids, host)).not.toThrow();
    expect(() => checkSpawnEnv({ NOTE: 'claude-sonnet-5-5' }, host)).toThrow(`spawn env "NOTE" refused: the value equals one of the Function's own environment values`);
  });
});

describe('checkProvisionEnv', () => {
  it("takes credential names and values, and a value of the Function's own env; still refuses a bad name, a NUL and a loader variable, naming the key only", () => {
    const token = `ghp_${'a'.repeat(36)}`;
    process.env.DH_TEST_PROVISION_TOKEN = token;
    try {
      expect(() => checkProvisionEnv({ GITHUB_TOKEN: token, NPM_TOKEN: 'npm-token-value', AUTH: `Bearer ${token}` })).not.toThrow();
    } finally {
      delete process.env.DH_TEST_PROVISION_TOKEN;
    }
    for (const env of [{ 'BAD-NAME': 'x' }, { NOTE: 'a\0b' }, { LD_PRELOAD: '/x.so' }, { BASH_ENV: '/x' }] as Record<string, string>[]) {
      const key = Object.keys(env)[0]!;
      expect(() => checkProvisionEnv(env)).toThrow(TypeError);
      expect(() => checkProvisionEnv(env)).toThrow(key);
    }
  });
});

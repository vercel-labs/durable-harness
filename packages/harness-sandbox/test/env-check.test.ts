import { describe, expect, it } from 'vitest';
import { checkSpawnEnv } from '../src/env-check.ts';

describe('checkSpawnEnv', () => {
  it("passes Claude Code's model ids when the host holds the same ones (next dev run from Claude Code), and still refuses a host value under another name", () => {
    const host = { ANTHROPIC_DEFAULT_SONNET_MODEL: 'claude-sonnet-5-5', ANTHROPIC_MODEL: 'claude-sonnet-5-5' };
    const ids = { ANTHROPIC_DEFAULT_OPUS_MODEL: 'claude-sonnet-5-5', ANTHROPIC_DEFAULT_SONNET_MODEL: 'claude-sonnet-5-5', ANTHROPIC_DEFAULT_HAIKU_MODEL: 'claude-sonnet-5-5' };
    expect(() => checkSpawnEnv(ids, host)).not.toThrow();
    expect(() => checkSpawnEnv({ NOTE: 'claude-sonnet-5-5' }, host)).toThrow(`spawn env "NOTE" refused: the value equals one of the Function's own environment values`);
  });
});

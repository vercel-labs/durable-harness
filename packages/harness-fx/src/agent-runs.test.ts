import { describe, expect, it } from 'vitest';
import { fxModelOptions } from './agent-runs.ts';

describe('fx trace tool definitions', () => {
  it('keeps only a tool definition, not its provider configuration', () => {
    const options = fxModelOptions('test/model', { prompt: [], tools: [{ name: 'remote_lookup', inputSchema: { type: 'object' }, args: { authorization: 'test-only-credential' }, headers: { 'x-api-key': 'test-only-credential' } }] });
    expect(options.tools).toEqual([{ name: 'remote_lookup', inputSchema: { type: 'object' } }]);
    expect(JSON.stringify(options)).not.toContain('test-only-credential');
  });
});

import { describe, expect, it } from 'vitest';
import { modelMessages, modelTools } from './agent-runs.ts';

describe('trace tool definitions', () => {
  it('removes SDK transport options from messages and parts without changing the prompt', () => {
    const prompt = [{ role: 'user', providerOptions: { authorization: 'test-only-credential' }, content: [{ type: 'text', text: 'hello', providerMetadata: { authorization: 'test-only-credential' } }] }];
    expect(modelMessages(prompt)).toEqual([{ role: 'user', content: [{ type: 'text', text: 'hello' }] }]);
    expect(JSON.stringify(prompt)).toContain('test-only-credential');
  });
  it('projects supported schemas without retaining provider credentials or transport configuration', () => {
    const schema = { type: 'object', properties: { query: { type: 'string' } } };
    const tools = [
      { type: 'function', function: { name: 'lookup', description: 'Look up a record', parameters: schema }, headers: { authorization: 'test-only-credential' } },
      { name: 'anthropic_lookup', input_schema: schema, authorization: 'test-only-credential' },
      { type: 'provider', id: 'openai.mcp', name: 'remote_lookup', args: { authorization: 'test-only-credential', headers: { 'x-api-key': 'test-only-credential' } } },
      { type: 'mcp', server_label: 'remote', authorization: 'test-only-credential' },
    ];
    const result = modelTools(tools);
    expect(result).toEqual([
      { name: 'lookup', description: 'Look up a record', inputSchema: schema },
      { name: 'anthropic_lookup', inputSchema: schema },
      { name: 'remote_lookup' },
    ]);
    expect(JSON.stringify(result)).not.toContain('test-only-credential');
    expect(tools[0]!.headers?.authorization).toBe('test-only-credential');
  });
});

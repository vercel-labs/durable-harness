import { describe, expect, it } from 'vitest';
import { gatewayModelOptions, gatewayModelResult, ResponseCapture } from './agent-runs.ts';

const sse = (events: unknown[]) => events.map((e) => `event: x\ndata: ${JSON.stringify(e)}\n\n`).join('');

describe("a CLI's model requests for Agent Runs", () => {
  it('keeps each tool\'s name, description and schema, and no credential a tool carries', () => {
    const schema = { type: 'object', properties: { q: { type: 'string' } } };
    const options = gatewayModelOptions({
      path: '/v1/responses',
      body: {
        model: 'openai/gpt-5.5',
        input: [{ role: 'user', content: 'hi' }],
        tools: [
          { type: 'function', name: 'lookup', description: 'Looks it up.', parameters: schema },
          { type: 'mcp', server_label: 'crm', server_url: 'https://mcp.example.com', authorization: 'Bearer mcp-secret', headers: { 'x-api-key': 'header-secret' } },
          { name: 'read', description: 'Reads.', input_schema: schema },
          { type: 'function', function: { name: 'write', description: 'Writes.', parameters: schema } },
        ],
      },
    });
    expect(options.tools).toEqual([
      { type: 'function', name: 'lookup', description: 'Looks it up.', inputSchema: schema },
      { name: 'read', description: 'Reads.', inputSchema: schema },
      { type: 'function', name: 'write', description: 'Writes.', inputSchema: schema },
    ]);
    expect(JSON.stringify(options)).not.toMatch(/mcp-secret|header-secret/);
  });
  it("reads Anthropic's Messages API (Claude Code)", () => {
    const options = gatewayModelOptions({
      path: '/v1/messages?beta=true',
      body: {
        model: 'anthropic/claude-haiku-4.5',
        system: [{ type: 'text', text: 'Be brief.' }],
        tools: [{ name: 'write_file', input_schema: {} }],
        messages: [
          { role: 'user', content: 'write it' },
          { role: 'assistant', content: [{ type: 'tool_use', id: 'toolu_1', name: 'write_file', input: { path: 'a' } }] },
          { role: 'user', content: [{ type: 'tool_result', tool_use_id: 'toolu_1', content: 'ok' }] },
        ],
      },
    });
    expect(options).toMatchObject({ provider: 'anthropic', modelId: 'anthropic/claude-haiku-4.5', instructions: 'Be brief.' });
    expect(options.messages).toEqual([
      { role: 'user', content: 'write it' },
      { role: 'assistant', content: [{ type: 'tool-call', toolCallId: 'toolu_1', toolName: 'write_file', input: { path: 'a' } }] },
      { role: 'tool', content: [{ type: 'tool-result', toolCallId: 'toolu_1', output: { type: 'text', value: 'ok' } }] },
    ]);
    const { result } = gatewayModelResult(
      '/v1/messages',
      sse([
        { type: 'message_start', message: { id: 'msg_1', model: 'claude-haiku-4-5', usage: { input_tokens: 10, cache_read_input_tokens: 90, cache_creation_input_tokens: 5 } } },
        { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } },
        { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'Writing.' } },
        { type: 'content_block_start', index: 1, content_block: { type: 'tool_use', id: 'toolu_2', name: 'write_file', input: {} } },
        { type: 'content_block_delta', index: 1, delta: { type: 'input_json_delta', partial_json: '{"path":' } },
        { type: 'content_block_delta', index: 1, delta: { type: 'input_json_delta', partial_json: '"b"}' } },
        { type: 'message_delta', delta: { stop_reason: 'tool_use' }, usage: { output_tokens: 7 } },
      ])
    );
    expect(result).toEqual({
      finishReason: 'tool-calls',
      usage: { inputTokens: 105, outputTokens: 7, inputTokenDetails: { cacheReadTokens: 90, cacheWriteTokens: 5 } },
      responseId: 'msg_1',
      responseModelId: 'claude-haiku-4-5',
      content: [
        { type: 'text', text: 'Writing.' },
        { type: 'tool-call', callId: 'toolu_2', toolName: 'write_file', input: { path: 'b' } },
      ],
    });
  });

  it("reads OpenAI's Responses API (Codex)", () => {
    const options = gatewayModelOptions({
      path: '/v1/responses',
      body: {
        model: 'openai/gpt-5.4-mini',
        instructions: 'Be brief.',
        input: [
          { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'list files' }] },
          { type: 'function_call', call_id: 'call_1', name: 'shell', arguments: '{"cmd":"ls"}' },
          { type: 'function_call_output', call_id: 'call_1', output: 'a.txt' },
        ],
      },
    });
    expect(options).toMatchObject({ provider: 'openai', modelId: 'openai/gpt-5.4-mini', instructions: 'Be brief.' });
    expect(options.messages).toEqual([
      { role: 'user', content: [{ type: 'text', text: 'list files' }] },
      { role: 'assistant', content: [{ type: 'tool-call', toolCallId: 'call_1', toolName: 'shell', input: { cmd: 'ls' } }] },
      { role: 'tool', content: [{ type: 'tool-result', toolCallId: 'call_1', output: { type: 'text', value: 'a.txt' } }] },
    ]);
    const { result } = gatewayModelResult(
      '/v1/responses',
      sse([
        { type: 'response.output_text.delta', delta: 'One file.' },
        { type: 'response.output_item.done', item: { type: 'function_call', call_id: 'call_2', name: 'shell', arguments: '{"cmd":"cat a.txt"}' } },
        { type: 'response.completed', response: { id: 'resp_1', model: 'gpt-5.4-mini', status: 'completed', output: [{ type: 'function_call' }], usage: { input_tokens: 50, input_tokens_details: { cached_tokens: 40 }, output_tokens: 6 } } },
      ])
    );
    expect(result).toMatchObject({
      finishReason: 'tool-calls',
      usage: { inputTokens: 50, outputTokens: 6, inputTokenDetails: { cacheReadTokens: 40 } },
      responseId: 'resp_1',
      content: [
        { type: 'text', text: 'One file.' },
        { type: 'tool-call', callId: 'call_2', toolName: 'shell', input: { cmd: 'cat a.txt' } },
      ],
    });
  });

  it('reads chat completions, tool calls streamed in pieces', () => {
    const { result } = gatewayModelResult(
      '/v1/chat/completions',
      sse([
        { id: 'c1', model: 'm', choices: [{ delta: { content: 'Hi' } }] },
        { id: 'c1', choices: [{ delta: { tool_calls: [{ index: 0, id: 'call_9', function: { name: 'f', arguments: '{"a"' } }] } }] },
        { id: 'c1', choices: [{ delta: { tool_calls: [{ index: 0, function: { arguments: ':1}' } }] }, finish_reason: 'tool_calls' }] },
        { id: 'c1', choices: [], usage: { prompt_tokens: 12, completion_tokens: 3 } },
      ]) + 'data: [DONE]\n\n'
    );
    expect(result).toMatchObject({
      finishReason: 'tool-calls',
      usage: { inputTokens: 12, outputTokens: 3 },
      content: [{ type: 'text', text: 'Hi' }, { type: 'tool-call', callId: 'call_9', toolName: 'f', input: { a: 1 } }],
    });
  });

  it("reads AI Gateway's language-model protocol (OpenCode), its model in a header, its cost", () => {
    const options = gatewayModelOptions({
      path: '/v3/ai/language-model',
      model: 'anthropic/claude-haiku-4.5',
      body: { prompt: [{ role: 'system', content: 'Be brief.' }, { role: 'user', content: [{ type: 'text', text: 'hi' }] }] },
    });
    expect(options).toEqual({ provider: 'gateway', modelId: 'anthropic/claude-haiku-4.5', instructions: 'Be brief.', messages: [{ role: 'user', content: [{ type: 'text', text: 'hi' }] }] });
    const { result, attributes } = gatewayModelResult(
      '/v3/ai/language-model',
      sse([
        { type: 'response-metadata', id: 'gen_1', modelId: 'anthropic/claude-haiku-4.5' },
        { type: 'text-delta', delta: 'Hello.' },
        { type: 'finish', finishReason: { unified: 'stop', raw: 'end_turn' }, usage: { inputTokens: { total: 20, cacheRead: 15 }, outputTokens: { total: 2 } }, providerMetadata: { gateway: { cost: '0.0001' } } },
      ])
    );
    expect(result).toMatchObject({ finishReason: 'stop', usage: { inputTokens: 20, outputTokens: 2, inputTokenDetails: { cacheReadTokens: 15 } }, responseId: 'gen_1', content: [{ type: 'text', text: 'Hello.' }] });
    expect(attributes['gen_ai.usage.cost']).toBe(0.0001);
  });

  it("keeps a long response's start and its end, where usage is", () => {
    const capture = new ResponseCapture(100, 200);
    capture.push(sse([{ type: 'message_start', message: { usage: { input_tokens: 1 } } }]));
    for (let i = 0; i < 50; i++) capture.push(sse([{ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'x' } }]));
    capture.push(sse([{ type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 50 } }]));
    const { result } = gatewayModelResult('/v1/messages', capture.text());
    expect(result.usage).toMatchObject({ inputTokens: 1, outputTokens: 50 });
    expect(result.finishReason).toBe('stop');
  });
});

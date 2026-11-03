import { describe, expect, it } from 'vitest';
import { BUILTINS_ON, checkConfig, codexConfig, toToml } from '../src/config.ts';

describe('Codex dynamic-tool transport', () => {
  it.each([false, true])('keeps journaled tools direct with builtins=%s', (builtins) => {
    const config = codexConfig({ model: 'openai/gpt-6.1-sol', baseUrl: 'http://127.0.0.1:1234/v1', envKey: 'KEY', contextWindow: 272000, ...(builtins ? { extra: { features: BUILTINS_ON } } : {}) });
    expect(config.features).toMatchObject({ code_mode_host: false, code_mode: { enabled: true, direct_only_tool_namespaces: ['functions'] }, shell_tool: builtins, unified_exec: builtins, view_image: builtins, multi_agent: false, plugins: false });
    expect(config.sandbox_mode).toBe('read-only');
    expect(toToml(config)).toContain('code_mode_host = false');
    expect(toToml(config)).toContain('[features.code_mode]\nenabled = true\ndirect_only_tool_namespaces = ["functions"]');
  });

  it('keeps transport and built-in tool flags reserved to the harness', () => {
    expect(() => checkConfig({ features: { code_mode_host: false } })).toThrow('set by the harness');
    expect(() => checkConfig({ features: { code_mode: { direct_only_tool_namespaces: [] } } })).toThrow('set by the harness');
  });
});

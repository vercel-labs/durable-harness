import { readFileSync } from 'node:fs';
import { afterEach, describe, expect, it, vi } from 'vitest';

const peers = ['@opentelemetry/sdk-trace-base', '@opentelemetry/resources', '@opentelemetry/otlp-transformer', '@vercel/agent-runs'];
const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
afterEach(() => { for (const peer of peers) vi.doUnmock(peer); vi.resetModules(); });

describe('optional Agent Runs setup', () => {
  it('keeps the baseline package on Node 22 without installing tracing libraries', () => {
    expect(pkg.engines.node).toBe('>=22');
    for (const peer of peers) {
      expect(pkg.dependencies[peer]).toBeUndefined();
      expect(pkg.peerDependencies[peer]).toBeDefined();
      expect(pkg.peerDependenciesMeta[peer]).toEqual({ optional: true });
    }
    expect(pkg.devDependencies['@vercel/agent-runs']).toBe('0.0.1');
  });

  it.each(peers)('names the install line when %s is missing', async peer => {
    vi.doMock(peer, () => { throw Object.assign(new Error(`Cannot find package '${peer}'`), { code: 'ERR_MODULE_NOT_FOUND' }); });
    const { agentRuns } = await import('../src/instrumentation/otel.ts');
    expect(() => agentRuns()).toThrow(`${peer} is not installed`);
    expect(() => agentRuns()).toThrow(`pnpm add ${peers.map(name => `${name}@${pkg.devDependencies[name]}`).join(' ')}`);
  });
});

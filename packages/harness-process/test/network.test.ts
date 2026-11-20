import { describe, expect, it } from 'vitest';
import { gatewayNetwork, networkSummary } from '../src/network.ts';

/** What a harness derives from `sandbox({ network })`: the mode and the domains, never the rules. */
describe('networkSummary and gatewayNetwork', () => {
  it('keeps the shorthands as they were', () => {
    expect(networkSummary(undefined)).toBe('allow-all');
    expect(networkSummary('allow-all')).toBe('allow-all');
    expect(networkSummary('deny-all')).toBe('deny-all');
    expect(networkSummary({ allow: ['github.com'] })).toEqual({ allow: ['github.com'] });
    expect(gatewayNetwork(networkSummary({ allow: ['github.com'] }))).toBe('custom');
    expect(gatewayNetwork('allow-all')).toBe('allow-all');
    expect(gatewayNetwork('deny-all')).toBe('deny-all');
  });

  it('reduces a full policy to the domains it names, conservatively: never allow-all, no rules, no subnets', () => {
    const summary = networkSummary({
      allow: { 'api.github.com': [{ transform: [{ headers: { authorization: 'Bearer sk-live-secret' } }] }], '*': [] },
      subnets: { allow: ['10.0.0.0/8'] },
    });
    expect(summary).toEqual({ allow: ['api.github.com', '*'] });
    expect(JSON.stringify(summary)).not.toContain('sk-live');
    expect(gatewayNetwork(summary)).toBe('custom');
    expect(networkSummary({ subnets: { allow: ['10.0.0.0/8'] } })).toEqual({ allow: [] });
  });

  it('a callback is known only at attach; until then the gateway is as narrow as deny-all', () => {
    expect(networkSummary(() => 'allow-all')).toBeUndefined();
    expect(gatewayNetwork(undefined)).toBe('deny-all');
  });
});

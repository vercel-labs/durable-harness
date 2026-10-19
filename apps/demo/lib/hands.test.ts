import type { Harness } from 'harness-world';
import { afterEach, describe, expect, it, vi } from 'vitest';

// Whether a Vercel project is there to name the session's VMs for: the check behind sandboxOffReason.
const project = vi.hoisted(() => ({ linked: true }));
vi.mock('durable-harness/sandbox', () => ({
  sandboxName: (runId: string) => {
    if (!project.linked) throw new Error('harness-sandbox: no Vercel project for the sandbox');
    return `dh-${runId}`;
  },
}));

const { byHands, builtinOption, demoSandbox, handsOfConfig, sandboxOffReason, sandboxSessionUsd, startHands } = await import('./hands');
const { processHarnessOptions, processHarnesses } = await import('./harnesses');

const KEYS = ['VERCEL_ENV', 'HARNESS_SANDBOX_SESSION_USD', 'VERCEL_OIDC_TOKEN', 'AI_GATEWAY_API_KEY'] as const;
const saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]]));
afterEach(() => {
  project.linked = true;
  for (const k of KEYS) {
    if (saved[k] === undefined) delete process.env[k];
    else process.env[k] = saved[k];
  }
});

const option = (value: string) => processHarnesses().find((h) => h.value === value)!;
const listed = (value: string) => processHarnessOptions().find((h) => h.value === value)!;

describe('Claude Code and Codex: on wherever Vercel Sandbox is, production and preview included', () => {
  it('are on in a production or preview deployment (its OIDC token and project), with Sandbox or None hands', () => {
    for (const VERCEL_ENV of ['production', 'preview']) {
      process.env.VERCEL_ENV = VERCEL_ENV;
      for (const value of ['claude-code', 'codex']) {
        const o = listed(value);
        expect(o.disabledReason).toBeUndefined();
        expect(o.modes).toEqual(['server']);
        expect(o.hands).toEqual(['none', 'sandbox']);
        expect(o.defaultHands).toBe('sandbox');
        expect(o.handsDisabled?.virtual).toMatch(/runs only in a Vercel Sandbox/);
      }
    }
  });

  it('are off, saying why, without a project to make the VM in: `vercel link` locally, the system env vars on a deployment', () => {
    project.linked = false;
    delete process.env.VERCEL_ENV;
    expect(sandboxOffReason()).toMatch(/run `vercel link` in apps\/demo/);
    expect(listed('claude-code').disabledReason).toMatch(/vercel link/);
    expect(listed('codex').disabledReason).toMatch(/vercel link/);
    process.env.VERCEL_ENV = 'preview';
    expect(sandboxOffReason()).toMatch(/VERCEL_PROJECT_ID/);
    project.linked = true;
    expect(sandboxOffReason()).toBeUndefined();
  });

  it('Codex locally also needs a Gateway credential (a deployment has its OIDC token)', () => {
    delete process.env.VERCEL_ENV;
    delete process.env.VERCEL_OIDC_TOKEN;
    delete process.env.AI_GATEWAY_API_KEY;
    expect(listed('codex').disabledReason).toMatch(/AI Gateway credential/);
    process.env.VERCEL_ENV = 'production';
    expect(listed('codex').disabledReason).toBeUndefined();
  });

  it("caps each sandbox() session's model spend: $2, or HARNESS_SANDBOX_SESSION_USD", async () => {
    expect(sandboxSessionUsd()).toBe(2);
    expect((await demoSandbox()).budget.sessionUsd).toBe(2);
    process.env.HARNESS_SANDBOX_SESSION_USD = '0.5';
    expect((await demoSandbox()).budget.sessionUsd).toBe(0.5);
    for (const bad of ['0', '-1', 'lots', 'Infinity']) {
      process.env.HARNESS_SANDBOX_SESSION_USD = bad;
      expect(sandboxSessionUsd()).toBe(2);
    }
  });
});

describe('the sessions route checks the hands a page asks for', () => {
  it("records each harness's default when the page names none", () => {
    expect(startHands(option('claude-code'), undefined)).toEqual({ hands: 'sandbox' });
    expect(startHands(option('codex'), undefined)).toEqual({ hands: 'sandbox' });
    expect(startHands(option('opencode'), undefined)).toEqual({ hands: 'virtual' });
    expect(startHands(builtinOption('pi')!, undefined)).toEqual({ hands: 'virtual' });
    expect(startHands(builtinOption('pi')!, undefined, 'hybrid')).toEqual({ hands: 'machine' });
  });

  it('takes the hands each harness takes, and refuses the others with why', () => {
    expect(startHands(option('claude-code'), 'none')).toEqual({ hands: 'none' });
    expect(startHands(option('claude-code'), 'virtual')).toEqual({ error: expect.stringMatching(/runs only in a Vercel Sandbox/) });
    expect(startHands(option('codex'), 'machine')).toEqual({ error: expect.stringMatching(/runs only in a Vercel Sandbox/) });
    for (const hands of ['none', 'virtual', 'ephemeral', 'machine'] as const) {
      expect(startHands(option('opencode'), hands)).toEqual({ hands });
      expect(startHands(builtinOption('pi')!, hands)).toEqual({ hands });
      expect(startHands(builtinOption('fx')!, hands)).toEqual({ hands });
    }
    // The Function-tier harnesses' sandbox() isn't on this branch: off, never faked.
    for (const o of [option('opencode'), builtinOption('pi')!, builtinOption('fx')!]) expect(startHands(o, 'sandbox')).toEqual({ error: expect.stringMatching(/^Coming with/) });
    // A harness that names no hands: Virtual only.
    expect(startHands(option('ai-sdk'), 'none')).toEqual({ error: 'AI SDK doesn’t take None hands.' });
    expect(startHands(option('ai-sdk'), 'virtual')).toEqual({ hands: 'virtual' });
    expect(startHands(builtinOption('pi')!, 'hands')).toEqual({ error: 'unknown hands "hands"' });
  });

  it('refuses a machine without Vercel Sandbox credentials', () => {
    project.linked = false;
    delete process.env.VERCEL_ENV;
    expect(startHands(builtinOption('pi')!, 'machine')).toEqual({ error: expect.stringMatching(/vercel link/) });
    expect(startHands(builtinOption('pi')!, undefined, 'hybrid')).toEqual({ hands: 'virtual' });
  });
});

describe('each drive builds the harness for its session’s hands', () => {
  it('reads the hands its run_created recorded, else the ones its harness had before', () => {
    expect(handsOfConfig({ hands: 'ephemeral' }, 'pi')).toBe('ephemeral');
    expect(handsOfConfig({ durability: 'run-ahead' }, 'pi')).toBe('virtual');
    expect(handsOfConfig(undefined, 'claude-code')).toBe('none');
    expect(handsOfConfig({ hands: 'sideways' }, 'codex')).toBe('none');
  });

  it('builds one harness per hands, once, and sends each drive to its own', async () => {
    const built: string[] = [];
    const drove: string[] = [];
    const harness = byHands('pi', {}, async (hands) => {
      built.push(hands);
      return { name: 'pi', drive: async (ctx) => void drove.push(`${hands}:${ctx.runId}`) } satisfies Harness;
    });
    const ctx = (runId: string, config: unknown) => ({ runId, config }) as Parameters<Harness['drive']>[0];
    await harness.drive(ctx('a', { hands: 'none' }));
    await harness.drive(ctx('b', { hands: 'machine' }));
    await harness.drive(ctx('c', undefined));
    await harness.drive(ctx('a', { hands: 'none' }));
    expect(built).toEqual(['none', 'machine', 'virtual']);
    expect(drove).toEqual(['none:a', 'machine:b', 'virtual:c', 'none:a']);
  });

  it('declares how its model calls are recorded before the first build, and warns when the harness disagrees', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      const ctx = { runId: 'a', config: { hands: 'none' } } as Parameters<Harness['drive']>[0];
      const self = byHands('pi', { modelCalls: 'self' }, async () => ({ name: 'pi', modelCalls: 'self', drive: async () => {} }));
      expect(self.modelCalls).toBe('self');
      await self.drive(ctx);
      expect(warn).not.toHaveBeenCalled();
      const effects = byHands('pi', { modelCalls: 'self' }, async () => ({ name: 'pi', drive: async () => {} }));
      await effects.drive(ctx);
      expect(warn).toHaveBeenCalledWith(expect.stringContaining("declares modelCalls 'self', the harness 'effects'"));
    } finally {
      warn.mockRestore();
    }
  });
});

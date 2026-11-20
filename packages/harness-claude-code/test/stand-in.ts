/**
 * The session's sandbox for this package's tests: the sandbox tier's session
 * factory (`sandboxSession`, the real lifecycle: the `sandbox.attach` fact,
 * the fence, the `hw.sandbox` record, the relay armed and dialed) over the
 * stand-in platform (`harness-world-sandbox/testing/fake-api`: a local relay per
 * "VM", a temp dir per "Drive"), bound for the kernel (`kernelSandbox`). The
 * machine it hands out is branded by the factory's test seam, so the harness
 * accepts it; nothing here is published.
 */
import { kernelSandbox, type SandboxNetworkOption, sandboxSession } from 'harness-world-sandbox';
import { FakeSandboxApi } from 'harness-world-sandbox/testing/fake-api';
import type { ClaudeSandbox } from '../src/harness.ts';

export interface StandIn {
  api: FakeSandboxApi;
  sandbox: ClaudeSandbox;
}

export interface StandInOptions {
  api?: FakeSandboxApi;
  network?: SandboxNetworkOption;
  budget?: { turnUsd: number; sessionUsd: number };
  env?: Record<string, string>;
  commandTimeoutMs?: number;
  log?: (message: string, fields?: Record<string, unknown>) => void;
}

/** A sandbox binding over the stand-in platform (one factory: the platform outlives the invocations of a test). */
export function standIn(options: StandInOptions = {}): StandIn {
  process.env.DH_TEST_LOCAL_SANDBOX = '1';
  const api = options.api ?? new FakeSandboxApi();
  const network = options.network ?? 'allow-all';
  const acquire = sandboxSession({
    spec: { vcpus: 2, network, idleMs: 60_000, driveSize: 10 * 1024 ** 3, budget: { vmSeconds: Infinity }, image: 'stand-in', ...(options.commandTimeoutMs ? { commandTimeoutMs: options.commandTimeoutMs } : {}) },
    // The stand-in runs the degraded tier (one uid, no cgroups), and on macOS has no /proc for the model port's peer check.
    test: { api, allowDegraded: true, allowUnauthenticatedModel: true },
    ...(options.log ? { log: options.log } : {}),
  });
  return {
    api,
    sandbox: {
      ...kernelSandbox(acquire),
      network,
      budget: options.budget ?? { turnUsd: Infinity, sessionUsd: Infinity },
      ...(options.env ? { env: options.env } : {}),
      ...(options.commandTimeoutMs ? { commandTimeoutMs: options.commandTimeoutMs } : {}),
    },
  };
}

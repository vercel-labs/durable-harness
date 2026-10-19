import type { Harness } from 'harness-world';
import type { HarnessOption } from 'harness-world-demo-ui';
import { byHands } from '../hands';
import aiSdk from './ai-sdk';
import claudeCode from './claude-code';
import codex from './codex';
import opencode from './opencode';
import reference from './reference';
import type { DemoHarness } from './types';

export type { DemoHarness } from './types';

/**
 * The demo's process harnesses. To add one: a file next to this one that
 * default-exports a `DemoHarness`, and one line in this list.
 */
const PROCESS_HARNESSES: DemoHarness[] = [claudeCode, codex, opencode, aiSdk, reference];

/** The ones this deployment registers. */
export function processHarnesses(): DemoHarness[] {
  return PROCESS_HARNESSES.filter((h) => h.enabled?.() ?? true);
}

/** What the page's picker shows for them. */
export function processHarnessOptions(): HarnessOption[] {
  return processHarnesses().map(({ value, label, title, modes, model, durabilities, crashes, hands, defaultHands, handsDisabled, disabledReason }) => {
    const reason = disabledReason?.();
    return {
      value,
      label,
      title,
      modes,
      ...(model ? { model } : {}),
      ...(durabilities ? { durabilities } : {}),
      ...(crashes ? { crashes } : {}),
      ...(hands ? { hands } : {}),
      ...(defaultHands ? { defaultHands } : {}),
      ...(handsDisabled ? { handsDisabled } : {}),
      ...(reason ? { disabledReason: reason } : {}),
      process: true,
    };
  });
}

/**
 * The kernel harnesses, each built per hands on its first drive with them
 * (lib/hands.ts `byHands`). Each records its own model calls: the CLIs and
 * the reference CLI behind the Gateway proxy, the AI SDK from its requests.
 */
export function processKernelHarnesses(): Harness[] {
  return processHarnesses().map((h) => byHands(h.value, { modelCalls: 'self' }, (hands) => h.load(hands)));
}
